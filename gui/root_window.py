import tkinter as tk
import sys
import os
from load_config import open_directory
from make_config import create_config

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from util import get_versioning

version = get_versioning("versions.json").split("|")[0]

root = tk.Tk()

root.title(f"{version}")


def menu(root):
    menubar = tk.Menu(root)

    file_menu = tk.Menu(menubar, tearoff=0)
    file_menu.add_command(label="Load Existing Config", command=lambda: open_directory(root))
    file_menu.add_separator()
    file_menu.add_command(label="Exit", command=root.quit)
    menubar.add_cascade(label="File", menu=file_menu)

    window_menu = tk.Menu(menubar, tearoff=0)
    window_menu.add_command(label="Create New Config", command=lambda: create_config(root))
    menubar.add_cascade(label="Label Creation", menu=window_menu)

    root.config(menu=menubar)


menu(root)


root.mainloop()
