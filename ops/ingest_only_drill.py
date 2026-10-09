"""Synthetic local drill; containers/data are stopped and retained, never deleted.

Build a native binary with GOWORK=off go build -o /tmp/audit-drill ./cmd/csar-audit,
then run python3 ops/ingest_only_drill.py --binary /tmp/audit-drill.
Uses isolated fresh Docker fixtures, localhost clients and synthetic identities.
The native audit binary's temporary development listeners have TLS disabled.
"""
import argparse
import base64
import datetime
import json
import os
import pathlib
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid


def command(*args, timeout=30):
    result = subprocess.run(args, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"fixture command failed: {args[0]} exit={result.returncode}")
    return result.stdout.strip()


def wait_for(check, timeout=60):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            if check():
                return
        except (OSError, RuntimeError, ValueError):
            pass
        time.sleep(0.25)
    raise RuntimeError("fixture check timed out")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request(port, path, body=None, auth=None):
    headers = {"X-Gateway-Subject": "synthetic-operator"}
    if body is not None:
        body = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if auth:
        headers["Authorization"] = "Basic " + base64.b64encode(auth.encode()).decode()
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=body, headers=headers)
    try:
        response = urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req, timeout=6)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.code, response.read().decode()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    args = parser.parse_args()
    binary = str(pathlib.Path(args.binary).resolve(strict=True))
    os.umask(0o077)
    receipt = {"generation": "audit-ingest-drill-" + uuid.uuid4().hex[:10]}
    root = pathlib.Path(tempfile.mkdtemp(prefix=receipt["generation"] + "-"))
    names, process = [], None
    log = (root / "audit.log").open("w")

    def start_fixture(suffix, image, env, ports):
        name = receipt["generation"] + "-" + suffix
        options = ["docker", "create", "--name", name, "--label", "audit.synthetic=" + receipt["generation"],
                   "--memory", "1g", "--cpus", "1"]
        for value in env:
            options += ["-e", value]
        for port in ports:
            options += ["-p", f"127.0.0.1::{port}"]
        command(*options, image)
        names.append(name)
        command("docker", "start", name)
        mapping = json.loads(command("docker", "inspect", "--format", "{{json .NetworkSettings.Ports}}", name))
        return name, {p: int(mapping[f"{p}/tcp"][0]["HostPort"]) for p in ports}

    def stop_audit():
        nonlocal process
        if process and process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=35)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
                raise RuntimeError("audit did not shut down within its bounded deadline")
        process = None

    try:
        broker, ports = start_fixture("rabbit", "rabbitmq:4.3.6-management-alpine", [
            "RABBITMQ_DEFAULT_USER=audit-local", "RABBITMQ_DEFAULT_PASS=synthetic-only",
            "RABBITMQ_DEFAULT_VHOST=rabbitmq"], [5672, 15672])
        wait_for(lambda: request(ports[15672], "/api/vhosts", auth="audit-local:synthetic-only")[0] == 200)
        command("docker", "exec", broker, "rabbitmqctl", "set_policy", "-p", "rabbitmq", "audit-retain",
                "^audit\\.events(\\.dlq)?$", '{"delivery-limit":-1}', "--priority", "30", "--apply-to", "queues")
        http, health, grpc = free_port(), free_port(), free_port()
        def start_audit(ingest_only, dsn):
            nonlocal process
            config = root / ("paused.yaml" if ingest_only else "resumed.yaml")
            config.write_text(f"""service:
  name: audit-synthetic
  port: {http}
  health_port: {health}
grpc:
  port: {grpc}
database:
  ingest_only: {str(ingest_only).lower()}
  dsn: {json.dumps(dsn)}
rabbitmq:
  url: amqp://audit-local:synthetic-only@127.0.0.1:{ports[5672]}/rabbitmq
  reconnect_delay: 1s
ingest:
  receipt_timeout: 2s
consumer:
  batch_size: 20
  flush_interval: 100ms
""")
            process = subprocess.Popen([binary, "--config-source", "file", "--config-file", str(config)], stdout=log, stderr=log)
            wait_for(lambda: process.poll() is None and request(health, "/readiness")[0] == 200, timeout=25)

        def queue():
            code, body = request(ports[15672], "/api/queues/rabbitmq/audit.events", auth="audit-local:synthetic-only")
            if code != 200:
                raise RuntimeError("fixture queue metadata unavailable")
            return json.loads(body)

        events = [{"id": str(uuid.uuid4()), "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                   "actor": "synthetic", "service": "synthetic", "action": "drill.update", "target_type": "fixture",
                   "target_id": str(i), "scope_type": "platform"} for i in range(3)]
        start_audit(True, "deliberately invalid DSN")
        for _ in range(2):
            assert request(http, "/ingest", {"events": events})[0] == 202, "receipt not confirmed"
        wait_for(lambda: queue().get("messages_ready") == 6 and queue().get("consumers") == 0)
        assert queue().get("messages_unacknowledged") == 0
        assert request(http, "/admin/audit")[0] == 503
        assert "audit_persistence_paused 1" in request(health, "/metrics")[1]
        receipt["paused"] = {"confirmed_deliveries": 6, "consumers": 0, "unacknowledged": 0, "query_status": 503}
        command("docker", "stop", "--time", "10", broker)
        wait_for(lambda: request(health, "/readiness")[0] == 503, timeout=15)
        assert request(http, "/ingest", {"events": events[:1]})[0] == 503, "broker failure was accepted"
        stop_audit()
        command("docker", "start", broker)
        # Docker may allocate different ephemeral host ports after a restart.
        mapping = json.loads(command("docker", "inspect", "--format", "{{json .NetworkSettings.Ports}}", broker))
        ports = {p: int(mapping[f"{p}/tcp"][0]["HostPort"]) for p in [5672, 15672]}
        wait_for(lambda: queue().get("messages_ready") == 6)
        receipt["broker_restart"] = {"backlog_retained": 6, "unconfirmed_receipt_status": 503}
        pg, pg_ports = start_fixture("postgres", "postgres:18.6", ["POSTGRES_PASSWORD=synthetic-only", "POSTGRES_DB=audit_fixture"], [5432])
        # The image's bootstrap server accepts Unix sockets before the final
        # TCP server starts; waiting on that socket races the application ping.
        wait_for(lambda: command("docker", "exec", pg, "pg_isready", "-h", "127.0.0.1", "-U", "postgres", "-d", "audit_fixture") != "")
        start_audit(False, f"postgres://postgres:synthetic-only@127.0.0.1:{pg_ports[5432]}/audit_fixture?sslmode=disable")
        wait_for(lambda: command("docker", "exec", pg, "psql", "-U", "postgres", "-d", "audit_fixture", "-Atc", "SELECT count(*) FROM audit_events") == "3")
        wait_for(lambda: queue().get("messages_ready") == 0 and queue().get("messages_unacknowledged") == 0)
        assert "audit_persistence_paused 0" in request(health, "/metrics")[1]
        receipt["resumed"] = {"unique_rows": 3, "ready": 0, "unacknowledged": 0}
        receipt["passed"] = True
    finally:
        stop_audit()
        log.close()
        for name in reversed(names):
            label = command("docker", "inspect", "--format", '{{index .Config.Labels "audit.synthetic"}}', name)
            if label != receipt["generation"]:
                raise RuntimeError("fixture identity mismatch; refusing stop")
            command("docker", "stop", "--time", "10", name)
        receipt["retained_stopped_containers"] = names
        (root / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
        print(json.dumps({"receipt": str(root / "receipt.json"), **receipt}))


if __name__ == "__main__":
    main()
