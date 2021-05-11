import os
import configparser


def get_initial_state(obs_name=None, configfile=None):
    state = {}

    state['current_working_directory'] = os.getcwd()
    state['current_stack'] = {}
    try:
        state['current_stack']['obs_name'] = obs_name[0]
        state['current_stack']['proposal_id'] = \
            '-'.join(obs_name[0].split('-')[:-1])
    except IndexError:
        raise ValueError("No obs_name provided.")

    config = configparser.ConfigParser()
    if config.read(configfile) == [configfile]:
        pass
    else:
        raise IOError("Problem reading config file {}.".format(configfile))
    state['config'] = {s: dict(config[s]) for s in config.sections()}

    return state
