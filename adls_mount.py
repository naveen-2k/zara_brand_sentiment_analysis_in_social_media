dbutils.fs.mount(
  source = "wasbs://capstone-group-8@bootcampt.blob.core.windows.net/", 
  mount_point = "/mnt/capstone-group-8", 
  extra_configs = {"fs.azure.account.key.bootcampt.blob.core.windows.net": "<secret-key>"}
)

# Create folders inside the mounted container
dbutils.fs.mkdirs("/mnt/capstone-group-8/landing/post_detail")
dbutils.fs.mkdirs("/mnt/capstone-group-8/bronze/post_detail")
dbutils.fs.mkdirs("/mnt/capstone-group-8/archive/post_detail")


dbutils.fs.ls("/mnt/capstone-group-8")
