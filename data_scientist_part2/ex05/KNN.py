import pandas as pd
import os
from sklearn import preprocessing
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import f1_score
import sys
from sklearn.neighbors import KNeighborsClassifier

class KNN:
  def __init__(self):
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(cur_dir)
    self.csv_dir = os.path.join(parent_dir, 'sources/')
    self.table = 'items'
    if not os.path.exists(self.csv_dir):
        os.makedirs(self.csv_dir)
    self.filename_training = sys.argv[1] if len(sys.argv) > 2 else "Training_knight.csv"
    self.filename_test = sys.argv[2] if len(sys.argv) > 2 else "Validation_knight.csv"
    self.filename_output = "KNN.txt"
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
        y_train = df_training['knight']

        df_test = pd.read_csv(self.filepath_test, sep=',')
        X_test = df_test.drop(columns=['knight'])
        y_test = df_test['knight']

        # Training the model
        K = []
        f1_scores = []

        for k in range(2, 21):
           clf = KNeighborsClassifier(n_neighbors=k)
           clf.fit(X_train, y_train)
           y_pred = clf.predict(X_test)
           f1_score_value = f1_score(y_test, y_pred, average='weighted')
           K.append(k)

           f1_scores.append(f1_score_value)
          
        # Evaluating the model
        for k, f1 in zip(K, f1_scores):
            print(f"F1 Score for K={k}: {f1}")

        # Plot f1 scores
        plt.plot(K, f1_scores, marker='o')
        plt.title('F1 Score vs K')
        plt.xlabel('K')
        plt.ylabel('F1 Score')
        plt.xticks(K)
        plt.grid()
        plt.show()

        with open(self.filepath_output, 'w') as f:
            for pred in y_pred:
                if pred == 0:
                    f.write('Sith\n')
                else:
                    f.write('Jedi\n')

    except Exception as e:
      print(f"Error: {e}")

def main():
  a = KNN()
  a.run()

if __name__ == "__main__":
  main()