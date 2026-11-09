import subprocess
import unittest
from unittest.mock import patch

import remote_client


class RemoteClientTests(unittest.TestCase):
    def test_home_pixi_is_the_only_default_remote_runtime(self):
        self.assertEqual(
            remote_client.RUNNER_ENV_DEFAULTS["BIOAGENT_SNAKEMAKE"],
            "/home/liufangzhou/.pixi/bin/snakemake",
        )
        self.assertEqual(
            remote_client.RUNNER_ENV_DEFAULTS["BIOAGENT_SEQKIT"],
            "/home/liufangzhou/.pixi/bin/seqkit",
        )
        self.assertEqual(
            remote_client.config.REMOTE_PYTHON,
            "/home/liufangzhou/.pixi/envs/snakemake/bin/python",
        )
        self.assertEqual(
            remote_client.config.REMOTE_CONTROL_PYTHON,
            "/home/liufangzhou/.pixi/envs/snakemake/bin/python",
        )

    def test_ssh_disables_forwarding_and_uses_a_private_control_socket(self):
        with patch.object(remote_client.config, "SSH_HOST", "Cyclone"):
            client = remote_client.RemoteWorkerClient()
        command = client._ssh()
        self.assertIn("-T", command)
        self.assertIn("ClearAllForwardings=yes", command)
        self.assertIn("ControlMaster=no", command)
        self.assertTrue(
            any(value.startswith("ControlPath=/tmp/bioagent-ssh-") for value in command)
        )

    def test_remote_runtime_is_disabled_without_ssh_host(self):
        with patch.object(remote_client.config, "SSH_HOST", ""):
            client = remote_client.RemoteWorkerClient()
        self.assertFalse(client.enabled)

    def test_master_bootstrap_uses_persistent_private_socket(self):
        with patch.object(remote_client.config, "SSH_HOST", "Cyclone"):
            client = remote_client.RemoteWorkerClient()
        missing = subprocess.CompletedProcess([], 1, stdout=b"", stderr=b"")
        with (
            patch("remote_client.subprocess.run", return_value=missing),
            patch.object(client, "_run") as run,
        ):
            client._ensure_master()
        command = run.call_args.args[0]
        self.assertIn("ControlMaster=yes", command)
        self.assertIn("ControlPersist=600", command)
        self.assertIn("-MNf", command)

    def test_existing_content_addressed_release_is_reused(self):
        with patch.object(remote_client.config, "SSH_HOST", "Cyclone"):
            client = remote_client.RemoteWorkerClient()
        ready = subprocess.CompletedProcess([], 0, stdout=b"ready\n", stderr=b"")
        with (
            patch.object(client, "_version", return_value="abc"),
            patch.object(client, "_ensure_master"),
            patch.object(client, "_run", return_value=ready) as run,
        ):
            first = client.ensure_deployed()
            second = client.ensure_deployed()
        self.assertEqual(first, second)
        self.assertEqual(first[0], "abc")
        run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
