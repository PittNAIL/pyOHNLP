from tkinter import filedialog
import json

#TODO: Edit loaded config

def open_directory():
    directory = filedialog.askdirectory()
    if directory:
        file = filedialog.askopenfilename()
        load_config(file)


def load_config(file):
    with open(file, "r") as f:
        config = json.load(f)
    return config
