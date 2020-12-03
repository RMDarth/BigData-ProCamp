import subprocess


def main():
    result = subprocess.run(['hadoop fs -cat /bdpc/hadoop_mr/airline/output/part*'], shell=True, stdout=subprocess.PIPE)
    data = result.stdout.decode("utf-8").splitlines()
    airlines = {}
    for line in data:
        elems = line.split("\t")
        if len(elems) < 3:
            continue

        airlines[float(elems[2])] = elems

    result_count = 5
    for key in sorted(airlines.keys(), reverse=True):
        print(airlines[key])
        result_count -= 1
        if result_count == 0:
            break


if __name__ == '__main__':
    main()
