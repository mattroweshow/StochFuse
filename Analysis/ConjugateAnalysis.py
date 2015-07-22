# datasets = ["facebook", "boards", "reddit", "twitter"]
datasets = ["facebook"]

for dataset_name in datasets:
    # Read data from HDFS for the dataset - cleaned version, so that the vocabulary is restricted/reduced

    # Derive the burn-in period
    # get the min+max dates in the data
    # derive 25% burn in period

    # Derive the burn in period's delta distribution - univariate signals

    # Run shapiro-wilk test for normality - per-term univariate signals
    # write p-values to HDFS

    # Perform seqential updating to the model to detect significant deviations - and print those (single-tailed)

    # Identify colinear + co-contextual time-series deviations - and print those clusters



