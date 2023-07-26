import paramiko
import os
import time


class transfer:
    def __init__(self, remote_computer, remote_port, remote_base_dir,
                 remote_user, remote_pwd):
        self.remote_computer = remote_computer
        self.remote_port = remote_port
        self.remote_base_dir = remote_base_dir
        self.remote_user = remote_user
        self.remote_pwd = remote_pwd

    def send(self, transfer_file):
        start = time.time()
        try:
            t = paramiko.Transport(self.remote_computer, self.remote_port)

            t.connect(username=self.remote_user, password=self.remote_pwd)
            sftp = paramiko.SFTPClient.from_transport(t)

            base_name = os.path.basename(transfer_file)
            obsdate = base_name.split('_')[0][-8:]
            remote_path = os.path.join(self.remote_base_dir, obsdate, base_name)
            remote_path = remote_path.replace('\\', '/')

            print('sftp.put to:', remote_path, transfer_file)
            sftp.put(transfer_file, remote_path)

            sftp.close()
            t.close()
        except paramiko.ssh_exception.SSHException:
            return {'elaptime': time.time() - start, 'error': "SSHException"}

        return {'elaptime': time.time() - start, 'data': remote_path}
