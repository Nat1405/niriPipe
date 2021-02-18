import subprocess
import pkg_resources
import os
import configparser


def test_console_help():
    result = subprocess.run(['niriPipe', '--help'], stdout=subprocess.PIPE)
    assert b'NIRI' in result.stdout
    assert result.returncode == 0


def test_default_config_present():
    config = configparser.ConfigParser()
    config.read(pkg_resources.resource_filename(
                'niriPipe',
                os.path.join('cfg', 'default_config.cfg')))

    assert 'DATARETRIEVAL' in config.sections()
