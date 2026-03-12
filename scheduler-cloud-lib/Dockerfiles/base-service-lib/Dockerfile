ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""
FROM ${REGISTRY}${NAMESPACE}builder${TAG}
ARG DOCK_DIR=""

COPY ${DOCK_DIR}bin/* ${IGNIS_HOME}/bin/
RUN chmod +x ${IGNIS_HOME}/bin/*

COPY ${DOCK_DIR}env.d/* ${IGNIS_HOME}/env.d/
#COPY ${DOCK_DIR}etc/* ${IGNIS_HOME}/etc/

RUN ${IGNIS_HOME}/bin/ignis-base-service-install.sh