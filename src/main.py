from app.processes import process
from app import logger


def run_in_loop():
    while True:
        try:
            process()
        except Exception as e:
            logger.exception(e)


def main():
    run_in_loop()


if __name__ == "__main__":
    main()
