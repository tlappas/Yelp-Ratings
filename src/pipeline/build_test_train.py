from sklearn.model_selection import test_train_split

#Assumes labels are yelp ratings [1,2,3,4,5]. Remap must be a list of length 5.
def remap_labels(labels, new_labels):
    return [new_labels[label-1] for label in labels]

def split(all_data, labels):
    return x_train, x_test, y_train, y_test = test_train_split(all_data, test_size=0.3, stratify=labels)
