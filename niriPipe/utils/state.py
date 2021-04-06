import os
import configparser
import pkg_resources


def get_initial_state(obs_name=None, intent=None, configfile=None):
    """
    Get initial pipeline state.

    Note that arguments like 'obs_name', 'intent', etc. come passed in
    as lists.
    """
    state = {}

    state['current_working_directory'] = os.getcwd()
    state['current_stack'] = {}
    try:
        state['current_stack']['obs_name'] = obs_name[0]
        state['current_stack']['proposal_id'] = \
            '-'.join(obs_name[0].split('-')[:-1])
        state['current_stack']['intent'] = intent[0]
    except IndexError:
        raise ValueError("Insufficient metadata provided for stack.")

    # Read config from a file.
    # User provided config is most important, and overrides everything.
    #   - Read basic defaults from 'default_config.cfg'
    #   - If it's a standard not science stack, override with defaults from
    #     'default_config_calibration.cfg'
    #   - Finally, if a user provides a config file, override from that.
    config = configparser.ConfigParser()

    # Read basic default config.
    config.read(pkg_resources.resource_filename(
            'niriPipe',
            os.path.join('cfg', 'default_config.cfg')))

    # Standard star stacks need a few tweaks.
    if intent and intent[0] == 'calibration':
        config.read(pkg_resources.resource_filename(
            'niriPipe',
            os.path.join('cfg', 'default_config_calibration.cfg')))

    # Finally, override with user-provided configuration.
    if configfile:
        config.read(configfile[0])
    state['config'] = {s: dict(config[s]) for s in config.sections()}

    return state
