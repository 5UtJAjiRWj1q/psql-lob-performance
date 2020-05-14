# Compare LOB read performance on postgres

Testet on my Laptop i7-8565U with SSD, 300MB content with 5 iterations

read file                3488,4 MiB/s  0.433s
read bytea               78,8 MiB/s  19.037s
read bytea_external      79,4 MiB/s  18.904s
read lo                  36,6 MiB/s  40.969s
