from tkinter import *    # Carga módulo tk (widgets estándar)
from tkinter import ttk  # Carga ttk (para widgets nuevos 8.5+)


def main():
    print('started')
    root = Tk()
    root.geometry('300x200')
    combo = ttk.Combobox( state="readonly",values=['a','b','c','d'])
    combo.place(x=50, y=50)
    root.mainloop()
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()