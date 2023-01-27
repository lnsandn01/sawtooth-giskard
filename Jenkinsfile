#!groovy

// Copyright 2017 Intel Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ------------------------------------------------------------------------------

pipeline {
    agent {
        node {
            label 'master'
            customWorkspace "workspace/${env.BUILD_TAG}"
        }
    }

    triggers {
        cron(env.BRANCH_NAME == 'main' ? 'H 3 * * *' : '')
    }

    options {
        timestamps()
        buildDiscarder(logRotator(daysToKeepStr: '31'))
    }

    environment {
        ISOLATION_ID = sh(returnStdout: true, script: 'printf $BUILD_TAG | sha256sum | cut -c1-64').trim()
        COMPOSE_PROJECT_NAME = sh(returnStdout: true, script: 'printf $BUILD_TAG | sha256sum | cut -c1-64').trim()
    }

    stages {
        stage('Check User Authorization') {
            steps {
                readTrusted 'bin/authorize-cicd'
                sh './bin/authorize-cicd "$CHANGE_AUTHOR" /etc/jenkins-authorized-builders'
            }
            when {
                not {
                    branch 'main'
                }
            }
        }

        stage('Fetch Tags') {
            steps {
                sh 'git fetch --tag'
            }
        }

        stage('Build Lint Dependencies') {
            steps {
                sh 'docker-compose up --abort-on-container-exit --build --force-recreate --renew-anon-volumes --exit-code-from giskard-common giskard-common'
                sh 'docker-compose down'
            }
        }

        stage('Run Lint') {
            steps {
                sh 'docker-compose -f ci/run-lint.yaml up --abort-on-container-exit --build --force-recreate --renew-anon-volumes --exit-code-from lint lint'
                sh 'docker-compose -f ci/run-lint.yaml up --abort-on-container-exit --build --force-recreate --renew-anon-volumes --exit-code-from bandit bandit'
                sh 'docker-compose -f ci/run-lint.yaml down'
            }
        }

        stage('Build Test Dependencies') {
            steps {
                sh 'docker-compose -f docker-compose-installed.yaml build'
            }
        }

        stage('Run Tests') {
            steps {
                sh 'INSTALL_TYPE="" ./bin/run_tests -i deployment'
            }
        }

        stage('Compile coverage report') {
            steps {
                sh 'docker run --rm -v $(pwd):/project/sawtooth-giskard sawtooth-giskard-tests:$ISOLATION_ID /bin/bash -c "cd coverage && coverage combine && coverage html -d html"'
            }
        }

        stage('Create git archive') {
            steps {
                sh '''
                    REPO=$(git remote show -n origin | grep Fetch | awk -F'[/.]' '{print $6}')
                    VERSION=`git describe --dirty`
                    git archive HEAD --format=zip -9 --output=$REPO-$VERSION.zip
                    git archive HEAD --format=tgz -9 --output=$REPO-$VERSION.tgz
                '''
            }
        }

        stage('Archive Build artifacts') {
            steps {
                sh 'docker-compose -f ci/copy-debs.yaml up'
                sh 'docker-compose -f ci/copy-debs.yaml down'
            }
        }
    }

    post {
        always {
            sh 'docker-compose down'
            sh 'docker-compose -f ci/run-lint.yaml down'
            sh 'docker-compose -f ci/copy-debs.yaml down'
        }
        success {
            archiveArtifacts '*.tgz, *.zip, build/debs/*.deb, build/bandit.html, coverage/html/*'
        }
        aborted {
            error "Aborted, exiting now"
        }
        failure {
            error "Failed, exiting now"
        }
    }
}
