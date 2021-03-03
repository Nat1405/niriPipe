from setuptools import setup
import time


if __name__ == "__main__":
    setup(
            version='0.0.dev' + str(int(time.time()))
        )
