# xdbc-python

### TODOs
Check and adapt `PYTHON_LIBRARY_` paths in CMakeLists.txt, potentially make them dynamic.

Sample cmds:
--------------------------------------
xdbc-server cmd:
docker exec -it xdbcserver bash -c "./xdbc-server/build/xdbc-server --network-parallelism=1 --read-partitions=1 --read-parallelism=1 -cnocomp --compression-parallelism=1 --buffer-size=1024 --dp=1 -p38192 -f2 --skip-deserializer=0 --tid="1736802455283585995" --system=csv --profiling-interval=3000"

xdbc-python cmd:
docker exec -it xdbcpython bash -c 'export LD_LIBRARY_PATH=$(python3.9 -c "import pyarrow, os; print(os.path.dirname(pyarrow.__file__))"):$LD_LIBRARY_PATH && python3.9 /workspace/tests/pandas_xdbc.py --env_name "PyXDBC Client" --table "iotm" --iformat 2 --buffer_size 1024 --bufferpool_size 38912 --sleep_time 1 --rcv_par 1 --ser_par 1 --write_par 1 --decomp_par 1 --transfer_id 1736717865385601516 --server_host xdbcserver --server_port "1234" --source csv --skip_ser 0 --debug 1'

