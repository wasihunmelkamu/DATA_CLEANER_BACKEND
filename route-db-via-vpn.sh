#!/bin/bash

# === CONFIGURATION ===
DB_IP="4.234.194.233"
VPN_INTERFACE=${1:-"tun0"}  # You can pass interface name as first argment or default to tun0

# === CHECK VPN INTERFACE ===
if ! ip link show "$VPN_INTERFACE" > /dev/null 2>&1; then
  echo "❌ VPN interface '$VPN_INTERFACE' not found. Make sure your VPN is connected."
  exit 1
fi

# === GET VPN GATEWAY ===
VPN_GATEWAY=$(ip route show dev "$VPN_INTERFACE" | grep -m1 -oP 'via \K[\d.]+')
if [ -z "$VPN_GATEWAY" ]; then
  echo "❌ Could not determine VPN gateway for interface $VPN_INTERFACE."
  exit 1
fi

# === ADD/REPLACE ROUTE FOR DB_IP VIA VPN ===
echo "🔄 Routing DB ($DB_IP) via VPN ($VPN_INTERFACE) -> $VPN_GATEWAY ..."
sudo ip route replace "$DB_IP" via "$VPN_GATEWAY" dev "$VPN_INTERFACE"

echo "✅ DB IP is now routed through VPN."
