import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt

class ConfusionMatrix:
  def __init__(self):
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(cur_dir)
    self.csv_dir = os.path.join(parent_dir, 'sources/')
    self.table = 'items'
    if not os.path.exists(self.csv_dir):
        os.makedirs(self.csv_dir)
    self.filename_test = "Test_knight.csv"
    self.filename_train = "Train_knight.csv"
    self.filename_prediction = "predictions.txt"
    self.filename_truth = "truth.txt"
    self.strong_features = ['Agility', 'Strength']
    self.weak_features = ['Grasping', 'Burst']
    self.filename_test = os.path.join(self.csv_dir, self.filename_test)
    self.filepath_train = os.path.join(self.csv_dir, self.filename_train)
    self.filepath_pred = os.path.join(self.csv_dir, self.filename_prediction)
    self.filepath_truth = os.path.join(self.csv_dir, self.filename_truth)


  def normalize_df(self, df):
     return (df-df.min())/(df.max()-df.min())
  
  def clean_txt_file(self, filepath):
    with open(filepath, "r") as file:
        content = file.read()
    array = content.split()
    # Replace Jedi with 1 and Sith with 0
    array = [1 if x == 'Jedi' else 0 for x in array]
    print(f"clean_txt_file Array: {array}")
    return array
  
  def confusion_matrix(self, true, pred):
    if len(true) != len(pred) or len(true) == 0:
        raise ValueError("Length mismatch")
    print(f"True + Pred: {true + pred}")
    classes = set(true + pred)
    print(f"Classes: {classes}")
    num_classes = len(classes)
    shape = (num_classes, num_classes)
    print(f"Shape: {shape}")
    mat = np.zeros(shape)
    print(f"Matrix: \n{mat}")
    n = max(len(true),len(pred))
    print(f"n: {n}")
    for i in range(num_classes):
        if i == 0:
           print("####> Looking for Sith in TRUE array<####")
        else:
            print("####> Looking for Jedi in TRUE array <####")
        for j in range(num_classes):
            if j == 0:
                print("====> Looking for Sith in PRED array <====")
            else:
                print("====> Looking for Jedi in PRED array <====")
            if i == j and i == 0:
               print("==============> True positive <==============")
            elif i == j and i == 1:
               print("==============> True negative <==============")
            elif i != j and i == 0:
                print("==============> False positive <==============")
            elif i != j and i == 1:
                print("==============> False negative <==============")
            for k in range(n):
                if true[k] == i:
                    if i == 0:
                      print(f"The {k+1}th element of true {true[k]} is a Sith!")
                    else:
                      print(f"The {k+1}th element of true {true[k]} is a Jedi!")
                    if pred[k] == j:
                        mat[i][j] = mat[i][j] + 1
                        if i == 0:
                          print(f"--> The {k+1}th element of pred {pred[k]} is a Sith!")
                        else:
                          print(f"--> The {k+1}th element of pred {pred[k]} is a Jedi!")
                        print(f"--> i = {i} and j = {j}")
                        print(f"OOOO--> Matrice updated to \n{mat} OOOO")
                    else:
                       print("XXXX--> Matrice not updated! XXXX")
    print(f"final Matrix: {mat}")
    return mat

  def plot_confusion_matrix(self, cm, classes, title='Confusion matrix'):
    plt.imshow(cm, interpolation='nearest', cmap=plt.cm.Blues)
    plt.title(title)
    plt.colorbar()

    # Add labels to the axes
    tick_marks = np.arange(len(classes))
    plt.xticks(tick_marks, classes, rotation=45)
    plt.yticks(tick_marks, classes)

    # Annotate each cell with the corresponding value
    for i in range(cm.shape[0]):
        for j in range(cm.shape[1]):
            plt.text(j, i, format(cm[i, j], '.0f'),
                     ha="center", va="center",
                     color="black")

    plt.ylabel('True label')
    plt.xlabel('Predicted label')
    plt.tight_layout()
    plt.show()

  def run(self):
    df_train = pd.read_csv(self.filepath_train, sep=',')
    df_train['knight'] = df_train['knight'].map({'Sith': 0, 'Jedi': 1})
    array_truth =self.clean_txt_file(self.filepath_truth)
    print(f"Array truth: {array_truth}")
    array_pred = self.clean_txt_file(self.filepath_pred)
    print(f"Array pred: {array_pred}")
    print("--------------------------------")
    try:
      confusion_matrix = self.confusion_matrix(array_truth, array_pred)
      confusion_matrix = np.flip(confusion_matrix)
      self.plot_confusion_matrix(confusion_matrix, ['Jedi', 'Sith'])
      print(f"Confusion matrix: \n{confusion_matrix}")
    except Exception as e:
      print(f"Error: {e}")

def main():
  a = ConfusionMatrix()
  a.run()

if __name__ == "__main__":
  main()