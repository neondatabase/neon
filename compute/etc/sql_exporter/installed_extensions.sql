SELECT av_ext.name as extension_name, ext.extversion AS installed_version
FROM pg_available_extensions av_ext
JOIN pg_extension ext ON av_ext.name = ext.extname;