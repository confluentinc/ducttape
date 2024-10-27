# Copyright 2014 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from contextlib import contextmanager
import logging
import os
from paramiko import SSHClient, SSHConfig, MissingHostKeyPolicy
from paramiko.ssh_exception import SSHException, NoValidConnectionsError
from dataclasses import dataclass
from collections import deque
from typing import Iterable, Iterator
import shutil
import signal
import socket
import stat
import tempfile
import warnings

from ducktape.utils.http_utils import HttpMixin
from ducktape.utils.util import wait_until
from ducktape.errors import DucktapeError


NUM_RING_LINES = 33


def check_ssh(method):
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except (SSHException, NoValidConnectionsError, socket.error) as e:
            if self._custom_ssh_exception_checks:
                self._log(logging.DEBUG, "caught ssh error", exc_info=True)
                self._log(logging.DEBUG, "starting ssh checks:")
                self._log(logging.DEBUG, "\n".join(repr(f) for f in self._custom_ssh_exception_checks))
                for func in self._custom_ssh_exception_checks:
                    func(e, self)
            raise
    return wrapper


class RemoteAccountSSHConfig(object):
    def __init__(self, host=None, hostname=None, user=None, port=None, password=None, identityfile=None,
                 connecttimeout=None, **kwargs):
        """Wrapper for ssh configs used by ducktape to connect to remote machines.

        The fields in this class are lowercase versions of a small selection of ssh config properties
        (see man page: "man ssh_config")
        """
        self.host = host
        self.hostname = hostname or 'localhost'
        self.user = user
        self.port = port or 22
        self.port = int(self.port)
        self.password = password
        self.identityfile = identityfile
        # None is default, and it means default TCP timeout will be used.
        self.connecttimeout = int(connecttimeout) if connecttimeout is not None else None

    @staticmethod
    def from_string(config_str):
        """Construct RemoteAccountSSHConfig object from a string that looks like

        Host the-host
            Hostname the-hostname
            Port 22
            User ubuntu
            IdentityFile /path/to/key
        """
        config = SSHConfig()
        config.parse(config_str.split("\n"))

        hostnames = config.get_hostnames()
        if '*' in hostnames:
            hostnames.remove('*')
        assert len(hostnames) == 1, "Expected hostnames to have single entry: %s" % hostnames
        host = hostnames.pop()

        config_dict = config.lookup(host)
        if config_dict.get("identityfile") is not None:
            # paramiko.SSHConfig parses this in as a list, but we only want a single string
            config_dict["identityfile"] = config_dict["identityfile"][0]

        return RemoteAccountSSHConfig(host, **config_dict)

    def to_json(self):
        return self.__dict__

    def __repr__(self):
        return str(self.to_json())

    def __eq__(self, other):
        return other and other.__dict__ == self.__dict__

    def __hash__(self):
        return hash(tuple(sorted(self.__dict__.items())))


class RemoteAccountError(DucktapeError):
    """This exception is raised when an attempted action on a remote node fails.
    """

    def __init__(self, account, msg):
        self.account_str = str(account)
        self.msg = msg

    def __str__(self):
        return "%s: %s" % (self.account_str, self.msg)


@dataclass
class RemoteCommandError(RemoteAccountError):
    """This exception is raised when a process run by ssh*() returns a non-zero exit status.
    """
    account: "RemoteAccount"
    exit_status: int
    cmd: str
    msg: str = ""
    stdout: Iterable[str] = []
    stderr: Iterable[str] = []

    def __str__(self):
        lines = [f"{self.account}: Command {self.cmd} returned non-zero exit status {self.exit_status}.{self.msg}"]
        if self.msg:
            lines.append(f"\tMessage: {self.msg}")
        if self.stdout:
            lines.append(f"\tStdout:")
            lines.extend(f"\t\t{line}" for line in self.stdout)
        if self.stderr:
            lines.append(f"\tStderr:")
            lines.extend(f"\t\t{line}" for line in self.stderr)
        return "\n".join(lines)


class RemoteAccount(HttpMixin):
    """RemoteAccount is the heart of interaction with cluster nodes,
    and every allocated cluster node has a reference to an instance of RemoteAccount.

    It wraps metadata such as ssh configs, and provides methods for file system manipulation and shell commands.

    Each operating system has its own RemoteAccount implementation.
    """

    def __init__(self, ssh_config, externally_routable_ip=None, logger=None, ssh_exception_checks=[]):
        # Instance of RemoteAccountSSHConfig - use this instead of a dict, because we need the entire object to
        # be hashable
        self.ssh_config = ssh_config

        # We don't want to rely on the hostname (e.g. 'worker1') having been added to the driver host's /etc/hosts file.
        # But that means we need to distinguish between the hostname and the value of hostname we use for SSH commands.
        # We try to satisfy all use cases and keep things simple by
        #   a) storing the hostname the user probably expects (the "Host" value in .ssh/config)
        #   b) saving the real value we use for running the SSH command
        self.hostname = ssh_config.host
        self.ssh_hostname = ssh_config.hostname

        self.user = ssh_config.user
        self.externally_routable_ip = externally_routable_ip
        self._logger = logger
        self.os = None
        self._ssh_client = None
        self._sftp_client = None
        self._custom_ssh_exception_checks = ssh_exception_checks

    @property
    def operating_system(self):
        return self.os

    @property
    def logger(self):
        if self._logger:
            return self._logger
        else:
            return logging.getLogger(__name__)

    @logger.setter
    def logger(self, logger):
        self._logger = logger

    def _log(self, level, msg, *args, **kwargs):
        msg = "%s: %s" % (str(self), msg)
        self.logger.log(level, msg, *args, **kwargs)

    @check_ssh
    def _set_ssh_client(self):
        client = SSHClient()
        client.set_missing_host_key_policy(IgnoreMissingHostKeyPolicy())

        self._log(logging.DEBUG, "ssh_config: %s" % str(self.ssh_config))

        client.connect(
            hostname=self.ssh_config.hostname,
            port=self.ssh_config.port,
            username=self.ssh_config.user,
            password=self.ssh_config.password,
            key_filename=self.ssh_config.identityfile,
            look_for_keys=False,
            timeout=self.ssh_config.connecttimeout)

        if self._ssh_client:
            self._ssh_client.close()
        self._ssh_client = client
        self._set_sftp_client()

    @property
    def ssh_client(self):
        if (self._ssh_client
                and self._ssh_client.get_transport()
                and self._ssh_client.get_transport().is_active()):
            try:
                transport = self._ssh_client.get_transport()
                transport.send_ignore()
            except Exception as e:
                self._log(logging.DEBUG, "exception getting ssh_client (creating new client): %s" % str(e))
                self._set_ssh_client()
        else:
            self._set_ssh_client()

        return self._ssh_client

    def _set_sftp_client(self):
        if self._sftp_client:
            self._sftp_client.close()
        self._sftp_client = self.ssh_client.open_sftp()

    @property
    def sftp_client(self):
        if not self._sftp_client:
            self._set_sftp_client()
        else:
            self.ssh_client  # test connection

        return self._sftp_client

    def close(self):
        """Close/release any outstanding network connections to remote account."""

        if self._ssh_client:
            self._ssh_client.close()
            self._ssh_client = None
        if self._sftp_client:
            self._sftp_client.close()
            self._sftp_client = None

    def __str__(self):
        r = ""
        if self.user:
            r += self.user + "@"
        r += self.hostname
        return r

    def __repr__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return other is not None and self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(tuple(sorted(self.__dict__.items())))

    def wait_for_http_service(self, port, headers, timeout=20, path='/'):
        """Wait until this service node is available/awake."""
        url = "http://%s:%s%s" % (self.externally_routable_ip, str(port), path)

        err_msg = "Timed out trying to contact service on %s. " % url + \
            "Either the service failed to start, or there is a problem with the url."
        wait_until(lambda: self._can_ping_url(url, headers), timeout_sec=timeout, backoff_sec=.25, err_msg=err_msg)

    def _can_ping_url(self, url, headers):
        """See if we can successfully issue a GET request to the given url."""
        try:
            self.http_request(url, "GET", None, headers, timeout=.75)
            return True
        except Exception:
            return False

    def available(self):
        # TODO: https://github.com/confluentinc/ducktape/issues/339
        # try:
        #     self.ssh_client
        # except Exception:
        #     return False
        # else:
        #     return True
        # finally:
        #     self.close()
        return True

    @check_ssh
    def ssh(self, cmd, allow_fail=False):
        """Run the given command on the remote host, and block until the command has finished running.

        :param cmd: The remote ssh command
        :param allow_fail: If True, ignore nonzero exit status of the remote command,
               else raise an ``RemoteCommandError``

        :return: The exit status of the command.
        :raise RemoteCommandError: If allow_fail is False and the command returns a non-zero exit status
        """
        self._log(logging.DEBUG, "Running ssh command: %s" % cmd)

        client = self.ssh_client
        stdin, stdout, stderr = client.exec_command(cmd)

        # Unfortunately we need to read over the channel to ensure that recv_exit_status won't hang. See:
        # http://docs.paramiko.org/en/2.0/api/channel.html#paramiko.channel.Channel.recv_exit_status
        stdout_lines = stdout.readlines()
        exit_status = stdout.channel.recv_exit_status()
        try:
            if exit_status != 0:
                if not allow_fail:
                    raise RemoteCommandError(self, cmd, exit_status, stdout=stdout_lines, stderr=stderr.readlines())
                else:
                    self._log(logging.DEBUG, "Running ssh command '%s' exited with status %d and message: %s" %
                              (cmd, exit_status, stderr.read()))
        finally:
            stdin.close()
            stdout.close()
            stderr.close()

        return exit_status

    @check_ssh
    def ssh_capture(self, cmd, allow_fail=False, callback=None, combine_stderr=True, timeout_sec=None):
        """Run the given command asynchronously via ssh, and return an SSHOutputIter object.

        Does *not* block

        :param cmd: The remote ssh command
        :param allow_fail: If True, ignore nonzero exit status of the remote command,
               else raise an ``RemoteCommandError``
        :param callback: If set, the iterator returns ``callback(line)``
               for each line of output instead of the raw output
        :param combine_stderr: If True, return output from both stderr and stdout of the remote process.
        :param timeout_sec: Set timeout on blocking reads/writes. Default None. For more details see
            http://docs.paramiko.org/en/2.0/api/channel.html#paramiko.channel.Channel.settimeout

        :return SSHOutputIter: object which allows iteration through each line of output.
        :raise RemoteCommandError: If ``allow_fail`` is False and the command returns a non-zero exit status
        """
        self._log(logging.DEBUG, "Running ssh command: %s" % cmd)

        client = self.ssh_client
        chan = client.get_transport().open_session(timeout=timeout_sec)

        chan.settimeout(timeout_sec)
        chan.exec_command(cmd)
        chan.set_combine_stderr(combine_stderr)

        stdin = chan.makefile('wb', -1)  # set bufsize to -1
        stdout = chan.makefile('r', -1)
        stderr = chan.makefile_stderr('r', -1)

        stdout_buff = deque(maxlen=NUM_RING_LINES)
        def output_generator():

            for line in iter(stdout.readline, ''):
                stdout_buff.append(line)
                if callback is None:
                    yield line
                else:
                    yield callback(line)
            try:
                exit_status = stdout.channel.recv_exit_status()
                if exit_status != 0:
                    if not allow_fail:
                        raise RemoteCommandError(self, cmd, exit_status, stdout=stdout_buff, stderr=stderr.readlines())
                    else:
                        self._log(logging.DEBUG, "Running ssh command '%s' exited with status %d and message: %s" %
                                  (cmd, exit_status, stderr.read()))
            finally:
                stdin.close()
                stdout.close()
                stderr.close()

        return SSHOutputIter(output_generator, stdout)

    @check_ssh
    def ssh_output(self, cmd, allow_fail=False, combine_stderr=True, timeout_sec=None):
        """Runs the command via SSH and captures the output, returning it as a string.

        :param cmd: The remote ssh command.
        :param allow_fail: If True, ignore nonzero exit status of the remote command,
               else raise an ``RemoteCommandError``
        :param combine_stderr: If True, return output from both stderr and stdout of the remote process.
        :param timeout_sec: Set timeout on blocking reads/writes. Default None. For more details see
            http://docs.paramiko.org/en/2.0/api/channel.html#paramiko.channel.Channel.settimeout

        :return: The stdout output from the ssh command.
        :raise RemoteCommandError: If ``allow_fail`` is False and the command returns a non-zero exit status
        """
        self._log(logging.DEBUG, "Running ssh command: %s" % cmd)

        client = self.ssh_client
        chan = client.get_transport().open_session(timeout=timeout_sec)

        chan.settimeout(timeout_sec)
        chan.exec_command(cmd)
        chan.set_combine_stderr(combine_stderr)

        stdin = chan.makefile('wb', -1)  # set bufsize to -1
        stdout = chan.makefile('r', -1)
        stderr = chan.makefile_stderr('r', -1)

        try:
            stdoutdata = stdout.read()
            exit_status = stdin.channel.recv_exit_status()
            if exit_status != 0:
                if not allow_fail:
                    raise RemoteCommandError(self, cmd, exit_status, stdout=stdoutdata.split("\n"), stderr=stderr.readlines())
                else:
                    self._log(logging.DEBUG, "Running ssh command '%s' exited with status %d and message: %s" %
                              (cmd, exit_status, stderr.read()))
        finally:
            stdin.close()
            stdout.close()
            stderr.close()
        self._log(logging.DEBUG, "Returning ssh command output:\n%s" % stdoutdata)
        return stdoutdata

    def alive(self, pid):
        """Return True if and only if process with given pid is alive."""
        try:
            self.ssh("kill -0 %s" % str(pid), allow_fail=False)
            return True
        except Exception:
            return False

    def signal(self, pid, sig, allow_fail=False):
        cmd = "kill -%d %s" % (int(sig), str(pid))
        self.ssh(cmd, allow_fail=allow_fail)

    def kill_process(self, process_grep_str, clean_shutdown=True, allow_fail=False):
        cmd = """ps ax | grep -i """ + process_grep_str + """ | grep -v grep | awk '{print $1}'"""
        pids = [pid for pid in self.ssh_capture(cmd, allow_fail=True)]

        if clean_shutdown:
            sig = signal.SIGTERM
        else:
            sig = signal.SIGKILL

        for pid in pids:
            self.signal(pid, sig, allow_fail=allow_fail)

    def java_pids(self, match):
        """
        Get all the Java process IDs matching 'match'.

        :param match:               The AWK expression to match
        """
        cmd = """jcmd | awk '/%s/ { print $1 }'""" % match
        return [int(pid) for pid in self.ssh_capture(cmd, allow_fail=True)]

    def kill_java_processes(self, match, clean_shutdown=True, allow_fail=False):
        """
        Kill all the java processes matching 'match'.

        :param match:               The AWK expression to match
        :param clean_shutdown:      True if we should shut down cleanly with SIGTERM;
                                    false if we should shut down with SIGKILL.
        :param allow_fail:          True if we should throw exceptions if the ssh commands fail.
        """
        cmd = """jcmd | awk '/%s/ { print $1 }'""" % match
        pids = [pid for pid in self.ssh_capture(cmd, allow_fail=True)]

        if clean_shutdown:
            sig = signal.SIGTERM
        else:
            sig = signal.SIGKILL

        for pid in pids:
            self.signal(pid, sig, allow_fail=allow_fail)

    def copy_between(self, src, dest, dest_node):
        """Copy src to dest on dest_node

        :param src: Path to the file or directory we want to copy
        :param dest: The destination path
        :param dest_node: The node to which we want to copy the file/directory

        Note that if src is a directory, this will automatically copy recursively.

        """
        # TODO: if dest is an existing file, what is the behavior?

        temp_dir = tempfile.mkdtemp()

        try:
            # TODO: deal with very unlikely case that src_name matches temp_dir name?
            # TODO: I think this actually works
            local_dest = self._re_anchor_basename(src, temp_dir)

            self.copy_from(src, local_dest)

            dest_node.account.copy_to(local_dest, dest)

        finally:
            if os.path.isdir(temp_dir):
                shutil.rmtree(temp_dir)

    def scp_from(self, src, dest, recursive=False):
        warnings.warn("scp_from is now deprecated. Please use copy_from")
        self.copy_from(src, dest)

    def _re_anchor_basename(self, path, directory):
        """Anchor the basename of path onto the given directory

        Helper for the various copy_* methods.

        :param path: Path to a file or directory. Could be on the driver machine or a worker machine.
        :param directory: Path to a directory. Could be on the driver machine or a worker machine.

        Example::

            path/to/the_basename, another/path/ -> another/path/the_basename
        """
        path_basename = path

        # trim off path separator from end of path
        # this is necessary because os.path.basename of a path ending in a separator is an empty string
        # For example:
        #   os.path.basename("the/path/") == ""
        #   os.path.basename("the/path") == "path"
        if path_basename.endswith(os.path.sep):
            path_basename = path_basename[:-len(os.path.sep)]
        path_basename = os.path.basename(path_basename)

        return os.path.join(directory, path_basename)

    @check_ssh
    def copy_from(self, src, dest):
        if os.path.isdir(dest):
            # dest is an existing directory, so assuming src looks like path/to/src_name,
            # in this case we'll copy as:
            #   path/to/src_name -> dest/src_name
            dest = self._re_anchor_basename(src, dest)

        if self.isfile(src):
            self.sftp_client.get(src, dest)
        elif self.isdir(src):
            # we can now assume dest path looks like: path_that_exists/new_directory
            os.mkdir(dest)

            # for obj in `ls src`, if it's a file, copy with copy_file_from, elif its a directory, call again
            for obj in self.sftp_client.listdir(src):
                obj_path = os.path.join(src, obj)
                if self.isfile(obj_path) or self.isdir(obj_path):
                    self.copy_from(obj_path, dest)
                else:
                    # TODO what about uncopyable file types?
                    pass

    def scp_to(self, src, dest, recursive=False):
        warnings.warn("scp_to is now deprecated. Please use copy_to")
        self.copy_to(src, dest)

    @check_ssh
    def copy_to(self, src, dest):

        if self.isdir(dest):
            # dest is an existing directory, so assuming src looks like path/to/src_name,
            # in this case we'll copy as:
            #   path/to/src_name -> dest/src_name
            dest = self._re_anchor_basename(src, dest)

        if os.path.isfile(src):
            # local to remote
            self.sftp_client.put(src, dest)
        elif os.path.isdir(src):
            # we can now assume dest path looks like: path_that_exists/new_directory
            self.mkdir(dest)

            # for obj in `ls src`, if it's a file, copy with copy_file_from, elif its a directory, call again
            for obj in os.listdir(src):
                obj_path = os.path.join(src, obj)
                if os.path.isfile(obj_path) or os.path.isdir(obj_path):
                    self.copy_to(obj_path, dest)
                else:
                    # TODO what about uncopyable file types?
                    pass

    @check_ssh
    def islink(self, path):
        try:
            # stat should follow symlinks
            path_stat = self.sftp_client.lstat(path)
            return stat.S_ISLNK(path_stat.st_mode)
        except Exception:
            return False

    @check_ssh
    def isdir(self, path):
        try:
            # stat should follow symlinks
            path_stat = self.sftp_client.stat(path)
            return stat.S_ISDIR(path_stat.st_mode)
        except Exception:
            return False

    @check_ssh
    def exists(self, path):
        """Test that the path exists, but don't follow symlinks."""
        try:
            # stat follows symlinks and tries to stat the actual file
            self.sftp_client.lstat(path)
            return True
        except IOError:
            return False

    @check_ssh
    def isfile(self, path):
        """Imitates semantics of os.path.isfile

        :param path: Path to the thing to check
        :return: True if path is a file or a symlink to a file, else False. Note False can mean path does not exist.
        """
        try:
            # stat should follow symlinks
            path_stat = self.sftp_client.stat(path)
            return stat.S_ISREG(path_stat.st_mode)
        except Exception:
            return False

    def open(self, path, mode='r'):
        return self.sftp_client.open(path, mode)

    @check_ssh
    def create_file(self, path, contents):
        """Create file at path, with the given contents.

        If the path already exists, it will be overwritten.
        """
        # TODO: what should semantics be if path exists? what actually happens if it already exists?
        # TODO: what happens if the base part of the path does not exist?

        with self.sftp_client.open(path, "w") as f:
            f.write(contents)

    _DEFAULT_PERMISSIONS = int('755', 8)

    @check_ssh
    def mkdir(self, path, mode=_DEFAULT_PERMISSIONS):
        self.sftp_client.mkdir(path, mode)

    def mkdirs(self, path, mode=_DEFAULT_PERMISSIONS):
        self.ssh("mkdir -p %s && chmod %o %s" % (path, mode, path))

    def remove(self, path, allow_fail=False):
        """Remove the given file or directory"""

        if allow_fail:
            cmd = "rm -rf %s" % path
        else:
            cmd = "rm -r %s" % path

        self.ssh(cmd, allow_fail=allow_fail)

    @contextmanager
    def monitor_log(self, log):
        """
        Context manager that returns an object that helps you wait for events to
        occur in a log. This checks the size of the log at the beginning of the
        block and makes a helper object available with convenience methods for
        checking or waiting for a pattern to appear in the log. This will commonly
        be used to start a process, then wait for a log message indicating the
        process is in a ready state.

        See ``LogMonitor`` for more usage information.
        """
        try:
            offset = int(self.ssh_output("wc -c %s" % log).split()[0])
        except Exception:
            offset = 0
        yield LogMonitor(self, log, offset)


class SSHOutputIter(object):
    """Helper class that wraps around an iterable object to provide has_next() in addition to next()
    """

    def __init__(self, iter_obj_func, channel_file=None):
        """
        :param iter_obj_func: A generator that returns an iterator over stdout from the remote process
        :param channel_file: A paramiko ``ChannelFile`` object
        """
        self.iter_obj_func = iter_obj_func
        self.iter_obj = iter_obj_func()
        self.channel_file = channel_file

        # sentinel is used as an indicator that there is currently nothing cached
        # If self.cached is self.sentinel, then next object from ier_obj is not yet cached.
        self.sentinel = object()
        self.cached = self.sentinel

    def __iter__(self):
        return self

    def next(self):
        if self.cached is self.sentinel:
            return next(self.iter_obj)
        next_obj = self.cached
        self.cached = self.sentinel
        return next_obj

    __next__ = next

    def has_next(self, timeout_sec=None):
        """Return True if next(iter_obj) would return another object within timeout_sec, else False.

        If timeout_sec is None, next(iter_obj) may block indefinitely.
        """
        assert timeout_sec is None or self.channel_file is not None, "should have descriptor to enforce timeout"

        prev_timeout = None
        if self.cached is self.sentinel:
            if self.channel_file is not None:
                prev_timeout = self.channel_file.channel.gettimeout()

                # when timeout_sec is None, next(iter_obj) will block indefinitely
                self.channel_file.channel.settimeout(timeout_sec)
            try:
                self.cached = next(self.iter_obj, self.sentinel)
            except socket.timeout:
                self.iter_obj = self.iter_obj_func()
                self.cached = self.sentinel
            finally:
                if self.channel_file is not None:
                    # restore preexisting timeout
                    self.channel_file.channel.settimeout(prev_timeout)

        return self.cached is not self.sentinel


class LogMonitor(object):
    """
    Helper class returned by monitor_log. Should be used as::

        with remote_account.monitor_log("/path/to/log") as monitor:
            remote_account.ssh("/command/to/start")
            monitor.wait_until("pattern.*to.*grep.*for", timeout_sec=5)

    to run the command and then wait for the pattern to appear in the log.
    """

    def __init__(self, acct, log, offset):
        self.acct = acct
        self.log = log
        self.offset = offset

    def wait_until(self, pattern, **kwargs):
        """
        Wait until the specified pattern is found in the log, after the initial
        offset recorded when the LogMonitor was created. Additional keyword args
        are passed directly to ``ducktape.utils.util.wait_until``
        """
        return wait_until(lambda: self.acct.ssh("tail -c +%d %s | grep '%s'" % (self.offset + 1, self.log, pattern),
                                                allow_fail=True) == 0, **kwargs)


class IgnoreMissingHostKeyPolicy(MissingHostKeyPolicy):
    """Policy for ignoring missing host keys.
    Many examples show use of AutoAddPolicy, but this clutters up the known_hosts file unnecessarily.
    """

    def missing_host_key(self, client, hostname, key):
        return
