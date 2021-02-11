import subprocess


def test_console_help():
    result = subprocess.run(['niriPipe', '--help'], stdout=subprocess.PIPE)
    assert b'NIRI' in result.stdout
    assert result.returncode == 0
