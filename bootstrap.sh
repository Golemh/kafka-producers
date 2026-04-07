#!/bin/bash
# bootstrap.sh — Producer setup
# Called by cloud-init after packages and Docker are ready.
# Reads configuration from /opt/bootstrap-config.json.

set -euo pipefail

CONFIG_FILE="/opt/bootstrap-config.json"
USERNAME=$(jq -r '.username' "$CONFIG_FILE")
BOOTSTRAP_SERVERS=$(jq -r '.bootstrap_servers' "$CONFIG_FILE")
BLUESKY_PRODUCER_IMAGE=$(jq -r '.bluesky_producer_image' "$CONFIG_FILE")
LOKI_ENDPOINT=$(jq -r '.loki_endpoint' "$CONFIG_FILE")
HOME_DIR="/home/${USERNAME}"

echo "=== Producer bootstrap ==="

# --- Docker Compose setup ---
cp "${HOME_DIR}/kafka-producers/docker-compose/producer-compose.yml" "${HOME_DIR}/docker-compose.yml"

# --- Write .env ---
cat > "${HOME_DIR}/.env" <<ENVEOF
KAFKA_BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS}
PRODUCER_USER_HOME=${HOME_DIR}
BLUESKY_PRODUCER_IMAGE=${BLUESKY_PRODUCER_IMAGE}
LOKI_ENDPOINT=${LOKI_ENDPOINT}
ENVEOF

# --- Fix ownership ---
chown -R "${USERNAME}:${USERNAME}" "${HOME_DIR}"

# --- Start producer ---
echo "  Starting producer stack..."
cd "${HOME_DIR}" && docker compose up -d --build

# --- Node exporter ---
install_node_exporter() {
  local version="1.10.2"
  echo "  Installing node_exporter v${version}..."
  curl -sL "https://github.com/prometheus/node_exporter/releases/download/v${version}/node_exporter-${version}.linux-amd64.tar.gz" \
    -o /tmp/node_exporter.tar.gz
  tar xzf /tmp/node_exporter.tar.gz -C /tmp
  mv "/tmp/node_exporter-${version}.linux-amd64/node_exporter" /usr/local/bin/
  rm -rf /tmp/node_exporter*

  cat > /etc/systemd/system/node-exporter.service <<'SVCEOF'
[Unit]
Description=Prometheus Node Exporter
After=network-online.target
[Service]
Type=simple
User=nobody
ExecStart=/usr/local/bin/node_exporter
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
SVCEOF
  systemctl enable --now node-exporter.service
}

# --- Spot watcher ---
install_spot_watcher() {
  echo "  Installing spot-watcher daemon..."
  cat > /opt/spot-watcher.sh <<WATCHEREOF
#!/bin/bash
TOKEN=\$(curl -sf -X PUT "http://169.254.169.254/latest/api/token" \\
  -H "X-aws-ec2-metadata-token-ttl-seconds: 30")
ACTION=\$(curl -sf -H "X-aws-ec2-metadata-token: \$TOKEN" \\
  "http://169.254.169.254/latest/meta-data/spot/instance-action" 2>/dev/null)
if [ \$? -eq 0 ] && [ -n "\$ACTION" ]; then
  logger -t spot-watcher "Interruption warning received: \$ACTION"
  cd /home/${USERNAME} && docker compose stop
  sync
  logger -t spot-watcher "Graceful shutdown complete"
fi
WATCHEREOF
  chmod +x /opt/spot-watcher.sh

  cat > /etc/systemd/system/spot-watcher.service <<'SVCEOF'
[Unit]
Description=EC2 Spot Interruption Watcher
After=network-online.target
[Service]
Type=simple
ExecStart=/bin/bash -c 'while true; do /opt/spot-watcher.sh; sleep 5; done'
Restart=always
[Install]
WantedBy=multi-user.target
SVCEOF
  systemctl enable --now spot-watcher.service
}

install_node_exporter
install_spot_watcher

echo "=== Producer bootstrap complete ==="
