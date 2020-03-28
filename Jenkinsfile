pipeline {
    agent {
        docker 'godatadriven/pyspark'
    }

    environment {
        SPARK_HOME = '/opt/miniconda3/lib/python3.7/site-packages/pyspark'
    }

    stages {

        stage('Install dependencies') {
            steps {
                sh 'pip install -r requirements.txt'
            }
        }

        stage('Test with flake8') {
            when {
                not {
                    branch 'master'
                }
            }

            steps {
                sh 'flake8 fching/ --config=flake8.cfg'
            }
        }

        stage('Test with pytest') {
            when {
                branch 'master'
            }

            steps {
                sh 'pytest -v'
            }
        }

    }
}