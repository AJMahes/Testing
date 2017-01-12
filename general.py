import os
import urllib.request


def create_project_dir(directory):
    if not os.path.exists(directory):
        print("Creating directory "+directory)
        os.makedirs(directory)
    else:
        print("Already contains a directory named: "+ directory)


def update_open_access_pmc():
    print("downloading file")
    ftp_file = "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.csv"
    file_name = "input/oa_file_list.csv"
    urllib.request.urlretrieve(ftp_file, file_name)
    print("done")


# The queue line is the csv file
def create_data_files(directory, ftp_url):
    processed = directory + '/processed.txt'
    if not os.path.isfile(processed):
        write_file(processed, ftp_url)


def write_file(path, data):
    f = open(path, 'w')
    f.write(data)
    f.close()


# Add ftp url to processed file
def append_to_file(path, data):
    with open(path, 'a') as file:
        file.write(data + "\n")


def delete_file_contents(path):
    with open(path, 'w'):
        pass


def file_to_set(file_name):
    results = set()
    with open(file_name, 'rt') as f:
        for line in f:
            results.add(line.replace('\n', ''))
    return results


def set_to_file(linkss, file):
    delete_file_contents(file)
    for link in links:
        append_to_file(file, link)
#create_project_dir("output")
#create_project_dir("input")
#update_open_access_pmc()