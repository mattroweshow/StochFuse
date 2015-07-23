__author__ = 'rowem'

from IO.Input import FileInput

datasets = ["facebook", "boards"]

# process each dataset and produce a TSV file from the read in data
for dataset_name in datasets:
    print "Processing data for: " + dataset_name

    # get the posts
    fileInput = FileInput(dataset_name)
    dataset = fileInput.readBoardsPosts(dataset_name)

    # write the dataset to a tsv file


