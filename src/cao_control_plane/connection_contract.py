"""Stable, process-loaded contract for CAO conversation MCP connections."""

from __future__ import annotations

# Bump this only when bridge behavior changes in a way that the complete MCP
# tool catalog digest cannot represent. A long-lived Python bridge retains this
# value in memory, so reading newer package files cannot impersonate a current
# connection contract.
CAO_CONVERSATION_PROXY_ABI_VERSION = 1
