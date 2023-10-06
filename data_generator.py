import names
import random
import datetime

# names.get_first_name()
# str(datetime.datetime.now().strftime("%d%H%M%S")

with open("sample_incremental2.csv", "w") as f:
    f.write("header1,header2,header3\n")

    for i in range(1, 1000000):
        a = "ID0000" + str(i)
        for n in range(random.randint(1, 3)):
            b, c = ("IX000" + str(n), "clusternode")
            f.write(f"{a},{b},{c}\n")
