pkg_name=$(jq -r .name package.json)
pkg_version=$(jq -r .version package.json)

echo "Processing $pkg_name@$pkg_version"

# Check if package version already exists
if npm view "$pkg_name@$pkg_version" --json --ignore-scripts >/dev/null 2>&1; then
	echo "Version $pkg_version of $pkg_name already exists. Incrementing patch version."
	npm version patch --no-git-tag-version
else
	echo "Version $pkg_version of $pkg_name does not exist. Publishing as is."
fi

# Publish the package
npm publish --ignore-scripts

cd - > /dev/null
