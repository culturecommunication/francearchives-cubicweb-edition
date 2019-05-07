#!groovy
node('debian_stretch') {
    stage('Setup') {
        if (env.DESCRIPTION) {
            currentBuild.description = env.DESCRIPTION
        }
        checkout scm
        if (env.DIFF_ID) {
            sh('hg phabread --stack ' + env.DIFF_ID + ' | hg import -')
        }
        sh 'npm ci'
        sh('sudo apt-get install -y poppler-utils')
    }
    stage('Lint') {
        parallel (
            'flake8': {
                sh 'tox -e flake8'
            },
            'ESLint': {
                sh 'npm run lint'
            }
        )
    }
    stage('Test') {
        parallel (
            'Python': {
                timeout(time: 1, unit: 'HOURS') {
                  withEnv(["PATH+POSTGRESQL=/usr/lib/postgresql/9.6/bin"]) {
                    sh 'tox -e py27'
                  }
                }
            },
            'test-cms': {
                sh 'npm run test-cms'
            },
        )
    }
}
