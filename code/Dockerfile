FROM amazonlinux:latest


# Replace the args to lock to a specific version
ARG GREENGRASS_RELEASE_VERSION=2.9.1
ARG GREENGRASS_ZIP_FILE=greengrass-${GREENGRASS_RELEASE_VERSION}.zip
ARG GREENGRASS_RELEASE_URI=https://d2s8p88vqu9w66.cloudfront.net/releases/${GREENGRASS_ZIP_FILE}
ARG GREENGRASS_ZIP_SHA256=greengrass.zip.sha256

# Greengrass Version
LABEL greengrass-version=${GREENGRASS_RELEASE_VERSION}

# Set up Greengrass v2 execution parameters
# TINI_KILL_PROCESS_GROUP allows forwarding SIGTERM to all PIDs in the PID group so Greengrass can exit gracefully

#override with env file during docker generation(defaults needed in case of missing env file)
ENV TINI_KILL_PROCESS_GROUP=1 \ 
    GGC_ROOT_PATH=/greengrass/v2 \
    PROVISION=false \
    AWS_REGION=us-east-1 \
    THING_NAME=default_thing_name \
    THING_GROUP_NAME=default_thing_group_name \
    TES_ROLE_NAME=default_tes_role_name \
    TES_ROLE_ALIAS_NAME=default_tes_role_alias_name \
    COMPONENT_DEFAULT_USER=default_component_user \
    DEPLOY_DEV_TOOLS=false \
    INIT_CONFIG=default_init_config \
    TRUSTED_PLUGIN=default_trusted_plugin_path \
    THING_POLICY_NAME=default_thing_policy_name
RUN env

# Entrypoint script to install and run Greengrass
COPY "greengrass-entrypoint.sh" /
COPY "${GREENGRASS_ZIP_SHA256}" /

# modify /etc/sudoers and copy configuration for runtime (launch after docker run)
COPY "modify-sudoers.sh" /
RUN  yum update && yum install -y sudo && chmod +x /modify-sudoers.sh && ./modify-sudoers.sh && rm /modify-sudoers.sh

# Install Greengrass v2 dependencies (python version for components and lambdas = 3.9)
RUN yum update -y && yum install -y tar unzip wget procps which shadow-utils && \
    yum install -y java-11-amazon-corretto-headless && \
    wget $GREENGRASS_RELEASE_URI && sha256sum -c ${GREENGRASS_ZIP_SHA256} && \
    rm -rf /var/cache/yum && \
    chmod +x /greengrass-entrypoint.sh && \
    mkdir -p /opt/greengrassv2 $GGC_ROOT_PATH && unzip $GREENGRASS_ZIP_FILE -d /opt/greengrassv2 && rm $GREENGRASS_ZIP_FILE && rm $GREENGRASS_ZIP_SHA256

# install gstreamer dependencies
COPY "gst-plugin-deps.sh" /
RUN sudo chmod +x /gst-plugin-deps.sh && ./gst-plugin-deps.sh ${PACKAGES_BASE_URL_UPDATED} ${PACKAGES_BASE_URL_AMLLEVEL} ${ARCH_UPDATED} ${ARCH_AMLLEVEL} && \
rm /gst-plugin-deps.sh   
 
## install gstreamer: stream raw images and video from the edge, the edge component is based on this software     
#gstreamer1 (higher fedora level(37) in order to support latest openssl(openssl latest is a system requirement from amazonlinnux 2023) 
COPY "gst-plugin-libs.sh" /
RUN  sudo chmod +x gst-plugin-libs.sh && ./gst-plugin-libs.sh ${PACKAGES_BASE_URL_UPDATED} ${ARCH_UPDATED} && rm /gst-plugin-libs.sh
 		         

ENTRYPOINT ["/greengrass-entrypoint.sh"]
