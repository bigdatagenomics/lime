#!/bin/sh

# do we have enough arguments?
if [ $# < 3 ]; then
    echo "Usage:"
    echo
    echo "./release.sh <release version> <development version>"
    exit 1
fi

# pick arguments
release=$1
devel=$2

# get current branch
branch=$(git status -bs | awk '{ print $2 }' | awk -F'.' '{ print $1 }' | head -n 1)

# manually edit and commit changelog changes
# see https://github.com/bigdatagenomics/adam/issues/936
#./scripts/changelog.sh $1 | tee CHANGES.md
#git commit -a -m "Modifying changelog."

commit=$(git log --pretty=format:"%H" | head -n 1)
echo "releasing from ${commit} on branch ${branch}"

git push origin ${branch}

git checkout -b maint-${release} ${branch}
git commit -a -m "Modifying pom.xml files for ${release} release."
mvn --batch-mode \
  -P distribution \
  -Dresume=false \
  -Dtag=lime-parent-${release} \
  -DreleaseVersion=${release} \
  -DdevelopmentVersion=${devel} \
  -DbranchName=lime-${release} \
  release:clean \
  release:prepare \
  release:perform

if [ $? != 0 ]; then
  echo "Releasing ${release} failed."
  exit 1
fi

# publish docs
#publish-scaladoc.sh ${release}

if [ $branch = "master" ]; then
  # if original branch was master, update versions on original branch
  git checkout ${branch}
  mvn versions:set -DnewVersion=${devel} \
    -DgenerateBackupPoms=false
  git commit -a -m "Modifying pom.xml files for new development after ${release} release."
  git push origin ${branch}
fi
