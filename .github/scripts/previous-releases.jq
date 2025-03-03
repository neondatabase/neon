# Expects response from https://docs.github.com/en/rest/releases/releases?apiVersion=2022-11-28#list-releases as input,
# with tag names `release` for storage, `release-compute` for compute and `release-proxy` for proxy releases.
# Extract only the `tag_name` field from each release object
[ .[].tag_name ]

# Transform each tag name into a structured object using regex capture
| reduce map(
    capture("^(?<full>release(-(?<component>proxy|compute))?-(?<version>\\d+))$")
    | {
        component: (.component // "storage"),  # Default to "storage" if no component is specified
        version: (.version | tonumber),        # Convert the version number to an integer
        full: .full                            # Store the full tag name for final output
      }
  )[] as $entry  # Loop over the transformed list

# Accumulate the latest (highest-numbered) version for each component
({};
 .[$entry.component] |= (if . == null or $entry.version > .version then $entry else . end))

# Convert the resulting object into an array of formatted strings
| to_entries
| map("\(.key)=\(.value.full)")

# Output each string separately
| .[]
