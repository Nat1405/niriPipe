class AutoReduction:
	def __init__(self):
		pass

	def runReduction(self, observationNumber):
		"""Reduces a NIRI imaging observation and the needed calibrations.
		Steps (as presented in https://niriimg-drtutorial.readthedocs.io/):
			- Setup the caldb service
			- Get instrument configurations
				- Several exp_time, filter, camera combinations are possible. We need
				  a distinct calibration set for each of these mutually exclusive instrument
				  configurations. We're making a calibration set and stack for
				  each of these configurations.
			- Create file lists
			- Create master darks
			- Create bad pixel masks
			- Create flat fields
			- Reduce the object frames.
		"""

		self.setupCaldb()
		self.makeFileLists()
		self.makeDark()
		self.makeBPM()
		self.makeFlat()