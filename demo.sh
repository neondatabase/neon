
cargo neon stop
rm -rf .neon
cargo neon init
cargo neon start

export TENANT_ID=31ad8e1bfec849f61227b4c09f480b5f
export TIMELINE_ID=514099e6b4fad0c0612b3a6b8cf785fb
export CHILD_TIMELINE_ID=85321f977b4f9579c5fce1948f484652

cargo neon tenant create --tenant-id=$TENANT_ID --timeline-id=$TIMELINE_ID

# Produce some:
# ./neonmq.py produce mytopic ohai

# Show consumption:
# ./neonmq.py consume mytopic 1

# for word in shipping faster with postgres ; do ./neonmq.py produce mytopic $word ; done

# Make a child
# cargo neon timeline branch --tenant-id=31ad8e1bfec849f61227b4c09f480b5f --timeline-id=85321f977b4f9579c5fce1948f484652 --ancestor-branch-name=main --branch-name=child1

# Produce to child
# ./neonmq.py produce mytopic ohai 85321f977b4f9579c5fce1948f484652
