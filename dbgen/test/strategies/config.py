"""hypothesis testing setup"""
import os
from hypothesis import settings, Verbosity

# Register three profiles for varying degrees of testing
settings.register_profile("ci", max_examples=1000)
settings.register_profile("dev", max_examples=100)
settings.register_profile("debug", max_examples=30, verbosity=Verbosity.verbose)
# Load the profile from the environmental variable
settings.load_profile(os.getenv("HYPOTHESIS_PROFILE", "default"))
