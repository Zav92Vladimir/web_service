from main import Main
import time


if __name__ == "__main__":
    time.sleep(10)  # wait for dbs starting accept tcp/ip. Bad decision to hardcode this but it's temporary.
    main = Main()
    main.run()

