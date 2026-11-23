FROM flink:1.20.1-java17

# Pre-create state dirs owned by the flink user so the named volumes mounted on top inherit
# the right ownership (checkpoints/savepoints are shared by JobManager and TaskManager).
USER root
RUN mkdir -p /opt/flink/checkpoints /opt/flink/savepoints /opt/flink/jobs \
 && chown -R flink:flink /opt/flink/checkpoints /opt/flink/savepoints /opt/flink/jobs
USER flink
