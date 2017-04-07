import time


def main():
    print('Let\'s wait 30sec...')
    for i in range(30):
        print(i)
        time.sleep(1)
    print('Time is up!')
    return

if __name__ == "__main__":
    main()
