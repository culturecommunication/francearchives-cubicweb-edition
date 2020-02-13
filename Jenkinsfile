pipeline {
  agent {label 'debian_stretch'}
  stages {
    stage('set build description') {
      when {
        expression { env.DESCRIPTION != null }
      }
      steps {
        script {
          currentBuild.description = env.DESCRIPTION
        }
      }
    }
    stage('apply differential') {
      when {
        expression { env.DIFF_ID != null }
      }
      steps {
        sh "hg phabread --stack " + env.DIFF_ID + " | hg import -"
      }
    }
    stage('Setup') {
      steps {
        sh "npm ci"
        sh "sudo apt-get install -y poppler-utils"
      }
    }
    stage('Lint') {
      parallel {
        stage('flake8') {
          steps {
            sh "tox -e flake8"
          }
        }
        stage('check-manifest') {
          steps {
            sh "tox -e check-manifest"
          }
        }
        stage('eslint') {
          steps {
            sh "npm run lint"
          }
        }
      }
    }
    stage('Test') {
      parallel {
        stage('Python') {
          steps {
            sh "tox -e py3"
          }
        }
        stage('test-cms') {
          steps {
            sh "npm run test-cms"
          }
        }
      }
    }
  }
}
