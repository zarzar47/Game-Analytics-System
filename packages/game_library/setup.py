from setuptools import setup, find_packages

setup(
    name="game_library",
    version="0.1.0",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "game_library": ["games.json"],
    },
    description="Shared game data library",
)
