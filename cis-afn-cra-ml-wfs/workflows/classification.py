!pip install xgboost
!pip install hyperopt
!pip install dotenv
import logging
logging.getLogger('py4j').setLevel(logging.CRITICAL)

from jobs import classification_training

def classification_train():
    result = classification_training()
    if result:
        print("Job executed successfully")
    else:
        print("Job failed")

if __name__ == "__main__":
    classification_train()
