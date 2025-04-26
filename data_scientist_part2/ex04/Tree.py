import pandas as pd
import os
from sklearn import preprocessing
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sklearn
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix
from sklearn.metrics import f1_score
import sys

class Tree:
  def __init__(self):
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(cur_dir)
    self.csv_dir = os.path.join(parent_dir, 'sources/')
    self.table = 'items'
    if not os.path.exists(self.csv_dir):
        os.makedirs(self.csv_dir)
    self.filename_training = sys.argv[1] if len(sys.argv) > 2 else "Training_knight.csv"
    self.filename_test = sys.argv[2] if len(sys.argv) > 2 else "Validation_knight.csv"
    self.filename_output = "Tree.txt"
    self.filepath_training = os.path.join(self.csv_dir, self.filename_training)
    self.filepath_test = os.path.join(self.csv_dir, self.filename_test)
    self.filepath_output = os.path.join(self.csv_dir, self.filename_output)


  def normalize_df(self, df):
    x = df.values
    min_max_scaler = preprocessing.MinMaxScaler()
    x_scaled = min_max_scaler.fit_transform(x)
    df_normalized = pd.DataFrame(x_scaled, columns=df.columns)
    return df_normalized

  def run(self):
    try:
        # Data Preparation
        # Splitting the Dataset
        df_training = pd.read_csv(self.filepath_training, sep=',')
        X_train = df_training.drop(columns=['knight'])
        columns = X_train.columns
        y_train = df_training['knight']

        df_test = pd.read_csv(self.filepath_test, sep=',')
        X_test = df_test.drop(columns=['knight'])
        y_test = df_test['knight']

        # Feature scaling ensures that all the features are on a similar scale
        scaler = StandardScaler()
        X_train = scaler.fit_transform(X_train)
        X_test = scaler.transform(X_test)

        # Building Random Forest Classifier
        classifier = RandomForestClassifier(n_estimators=10, random_state=42)
        classifier.fit(X_train, y_train)
        y_pred = classifier.predict(X_test)

        # Calculate the F1 score
        f1 = f1_score(y_test, y_pred, average='weighted')
        print(f"F1 Score: {f1}")

        conf_matrix = confusion_matrix(y_test, y_pred)

        plt.figure(figsize=(10, 7))
        sns.heatmap(conf_matrix, annot=True, fmt='g', cmap='Blues', cbar=False, 
            xticklabels=['Sith', 'Jedi'], yticklabels=['Sith', 'Jedi'])
        plt.title('Confusion Matrix')
        plt.xlabel('Predicted labels')
        plt.ylabel('True labels')
        plt.show()

        # Feature Importance
        feature_importances = classifier.feature_importances_
        plt.barh(columns, feature_importances, color='skyblue')
        plt.xlabel('Feature Importance')
        plt.title('Feature Importance from Random Forest')
        plt.show()

        # Display the tree graph
        cn = ['Sith', 'Jedi']
        _, ax = plt.subplots(nrows=1, ncols=1, figsize=(3, 3), dpi=600)
        sklearn.tree.plot_tree(classifier.estimators_[0], feature_names=columns, class_names=cn, filled=True, ax=ax)
        plt.title('Decision Tree from Random Forest')
        plt.show()

        # Wtrite the prediction in a Tree.txt file
        with open(self.filepath_output, 'w') as f:
            for pred in y_pred:
                if pred == 0:
                    f.write('Sith\n')
                else:
                    f.write('Jedi\n')






        
    except Exception as e:
      print(f"Error: {e}")

def main():
  a = Tree()
  a.run()

if __name__ == "__main__":
  main()