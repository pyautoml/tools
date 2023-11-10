#!/usr/bin/python3.11

__created__ = "10.11.2023"
__last_update__ = "10.11.2023"
__author__ = "https://github.com/pyautoml"

import sys
import redis
from typing import Any
from dataclasses import dataclass
from sshtunnel import SSHTunnelForwarder
from utils import get_configuration_path, load_json_data


@dataclass
class SshConnection:
    _configuration_path: str
    _tunnel: SSHTunnelForwarder = None

    def __post_init__(self) -> None:
        self._configuration = load_json_data(f"{get_configuration_path()}/{self._configuration_path}")
        self._ssh_host = self._configuration["database"]["redis"]["ssh_host"]
        self._ssh_port = self._configuration["database"]["redis"]["ssh_port"]
        self._ssh_user = self._configuration["database"]["redis"]["ssh_user"]
        self._redis_host = self._configuration["database"]["redis"]["redis_host"]
        self._redis_port = self._configuration["database"]["redis"]["redis_port"]
        self._ssh_key_path = self._configuration["database"]["redis"]["ssh_key_path"]

    def _open(self) -> SSHTunnelForwarder:
        try:
            return SSHTunnelForwarder(
                (self._ssh_host, self._ssh_port),
                ssh_username=self._ssh_user,
                ssh_pkey=self._ssh_key_path,
                remote_bind_address=(self._redis_host, self._redis_port),
            )
        except Exception as e:
            print(f"An error occurred: {e}", flush=True)
            sys.exit(1)
  
    def _close(self) -> None:
        self._tunnel.stop()
        self._tunnel.close()

    def _redis_client(self) -> redis.StrictRedis:
        try:
            self._tunnel = self._open()
            self._tunnel.start()
          
            if not self._tunnel.is_active:
                raise ValueError("Tunnel is not active. Please start the tunnel before creating the Redis client.")

            return redis.StrictRedis(
                host=self._redis_host, 
                port=self._tunnel.local_bind_port,
                decode_responses=True
            )
        except Exception as e:
          print(f"Closing redis ssh connection. An error occured: {e}", flush=True)
            self._tunnel.stop()
            self._tunnel.close()
          
    def _redis_get_value_via_key(self, key: str) -> [Any|None]:
          try:
            redis_client = self._redis_client()
            value = redis_client.get(key)
            return value
        except Exception as e:
            print(f"An error occurred: {e}", flush=True)
            return None

# usage example 
# if __name__ == "__main__":
#     ssh_tunnel = SshConnection("./connection/connection.json")
#     ssh_tunnel._open()
#     print("Tunnel opened")
#     redis_client = ssh_tunnel._redis_client()
#     # Looking for 'cat' keyword in Redis DB:
#     print(redis_client._redis_get_value_via_key('cat'))
#     ssh_tunnel._close()
#     print("Tunnel closed")
