import time
import logging
from functools import wraps


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время,
    если возникла ошибка. Использует наивный экспоненциальный рост
    времени повтора (factor) до граничного времени ожидания (border_sleep_time)
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            sleep_time = start_sleep_time
            flag = False
            while True:
                try:
                    result = func(*args, **kwargs)
                    if flag:
                        logging.info('Подключение восстановлено.')
                    return result
                except Exception as err:
                    logging.error(f"Ошибка при выполнении {func.__name__}, {err} "
                                  f"Пытаемся восстановить подключение через {sleep_time:.2f} секунд...")
                    time.sleep(sleep_time)
                    sleep_time = min(sleep_time * factor, border_sleep_time)
                    flag = True

        return inner

    return func_wrapper
