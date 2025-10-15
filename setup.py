import sys
from cx_Freeze import setup, Executable
setup(
    name="Hello",
    version="1.0",
    description="Description of your program",
    executables=[Executable("main.py")]
)