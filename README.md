# Research about the performance of Information Retrieval based features for Text Mining tasks

This repository contains the code used by Ruan de Medeiros Bahia to pursue his research conducted in the Final Paper of the Computer Engineering course in Univasf (Federal University of Vale do São Francisco).

## Requirements

The following software and libraries are required to run this project:

* [Elasticsearch](https://github.com/elastic/elasticsearch) 7.4.0 ([documentation](https://www.elastic.co/guide/en/elasticsearch/reference/7.4/index.html))
* [ArangoDB](https://github.com/arangodb/arangodb) 3.5.1 ([documentation](https://www.arangodb.com/docs/3.5/aql/index.html))
* [Zettair](http://www.seg.rmit.edu.au/zettair/index.html) 0.94 ([unofficial repository](https://github.com/ruanmed/zettair/) | [documentation](http://www.seg.rmit.edu.au/zettair/documentation.html))
* [elasticsearch-py](https://github.com/elastic/elasticsearch-py) 7.0.5 ([pip package](https://pypi.org/project/elasticsearch/) | [documentation](https://elasticsearch-py.readthedocs.io/))
* [python-arango](https://github.com/joowani/python-arango) 5.2.1 ([pip package](https://pypi.org/project/python-arango/) | [documentation](https://python-driver-for-arangodb.readthedocs.io/))

Newer versions might be compatible.

### Docker file

To use the Docker file the following requirements are needed:

* [Docker Community Edition](https://github.com/docker/docker-ce) >= 19.03.x
* [Docker Compose](https://github.com/docker/compose/) >= 1.23.x

To install Docker CE follow the [guide](https://docs.docker.com/install/#supported-platforms) for your platform at Docker Docs website.

To install Docker Compose follow the [guide](https://docs.docker.com/compose/install/) for your platform at Docker Docs website.

To use the docker-compose.yml file to install the project, run the following command:

```bash
docker-compose up
```

For instance, to install the latest version of Docker CE in Ubuntu follow the steps ahead (extracted directly from the guide).

#### Install using the repository

Before you install Docker Engine - Community for the first time on a new host machine, you need
to set up the Docker repository. Afterward, you can install and update Docker
from the repository.

##### Set up the repository

1.  Update the `apt` package index:

    ```bash
    $ sudo apt-get update
    ```

2.  Install packages to allow `apt` to use a repository over HTTPS:

    ```bash
    $ sudo apt-get install \
        apt-transport-https \
        ca-certificates \
        curl \
        gnupg-agent \
        software-properties-common
    ```

3.  Add Docker's official GPG key:

    ```bash
    $ curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
    ```

    Verify that you now have the key with the fingerprint
    `9DC8 5822 9FC7 DD38 854A  E2D8 8D81 803C 0EBF CD88`, by searching for the
    last 8 characters of the fingerprint.

    ```bash
    $ sudo apt-key fingerprint 0EBFCD88
    
    pub   rsa4096 2017-02-22 [SCEA]
          9DC8 5822 9FC7 DD38 854A  E2D8 8D81 803C 0EBF CD88
    uid           [ unknown] Docker Release (CE deb) <docker@docker.com>
    sub   rsa4096 2017-02-22 [S]
    ```

4.  Use the following command to set up the **stable** repository. To add the
    **nightly** or **test** repository, add the word `nightly` or `test` (or both)
    after the word `stable` in the commands below. [Learn about **nightly** and **test** channels](https://docs.docker.com/install/index.md).

    > **Note**: The `lsb_release -cs` sub-command below returns the name of your
    > Ubuntu distribution, such as `xenial`. Sometimes, in a distribution
    > like Linux Mint, you might need to change `$(lsb_release -cs)`
    > to your parent Ubuntu distribution. For example, if you are using
    >  `Linux Mint Tessa`, you could use `bionic`. Docker does not offer any guarantees on untested
    > and unsupported Ubuntu distributions.


    <ul class="nav nav-tabs">
      <li class="active"><a data-toggle="tab" data-target="#x86_64_repo">x86_64 / amd64</a></li>
      <li><a data-toggle="tab" data-target="#armhf_repo">armhf</a></li>
      <li><a data-toggle="tab" data-target="#arm64_repo">arm64</a></li>
      <li><a data-toggle="tab" data-target="#ppc64le_repo">ppc64le (IBM Power)</a></li>
      <li><a data-toggle="tab" data-target="#s390x_repo">s390x (IBM Z)</a></li>
    </ul>
    <div class="tab-content">
    <div id="x86_64_repo" class="tab-pane fade in active" markdown="1">

    ```bash
    $ sudo add-apt-repository \
       "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
       $(lsb_release -cs) \
       stable"
    ```

    </div>
    <div id="armhf_repo" class="tab-pane fade" markdown="1">

    ```bash
    $ sudo add-apt-repository \
       "deb [arch=armhf] https://download.docker.com/linux/ubuntu \
       $(lsb_release -cs) \
       stable"
    ```

    </div>
    <div id="arm64_repo" class="tab-pane fade" markdown="1">

    ```bash
    $ sudo add-apt-repository \
       "deb [arch=arm64] https://download.docker.com/linux/ubuntu \
       $(lsb_release -cs) \
       stable"
    ```

    </div>
    <div id="ppc64le_repo" class="tab-pane fade" markdown="1">

    ```bash
    $ sudo add-apt-repository \
       "deb [arch=ppc64el] https://download.docker.com/linux/ubuntu \
       $(lsb_release -cs) \
       stable"
    ```

    </div>
    <div id="s390x_repo" class="tab-pane fade" markdown="1">

    ```bash
    $ sudo add-apt-repository \
       "deb [arch=s390x] https://download.docker.com/linux/ubuntu \
       $(lsb_release -cs) \
       stable"
    ```

    </div>
    </div> <!-- tab-content -->

##### Install Docker Engine - Community

1.  Update the `apt` package index.

    ```bash
    $ sudo apt-get update
    ```

2.  Install the _latest version_ of Docker Engine - Community and containerd, or go to the next step to install a specific version:

    ```bash
    $ sudo apt-get install docker-ce docker-ce-cli containerd.io
    ```

    > Got multiple Docker repositories?
    >
    > If you have multiple Docker repositories enabled, installing
    > or updating without specifying a version in the `apt-get install` or
    > `apt-get update` command always installs the highest possible version,
    > which may not be appropriate for your stability needs.

3.  To install a _specific version_ of Docker Engine - Community, list the available versions in the repo, then select and install:

    a. List the versions available in your repo:

    ```bash
    $ apt-cache madison docker-ce

      docker-ce | 5:18.09.1~3-0~ubuntu-xenial | https://download.docker.com/linux/ubuntu  xenial/stable amd64 Packages
      docker-ce | 5:18.09.0~3-0~ubuntu-xenial | https://download.docker.com/linux/ubuntu  xenial/stable amd64 Packages
      docker-ce | 18.06.1~ce~3-0~ubuntu       | https://download.docker.com/linux/ubuntu  xenial/stable amd64 Packages
      docker-ce | 18.06.0~ce~3-0~ubuntu       | https://download.docker.com/linux/ubuntu  xenial/stable amd64 Packages
      ...
    ```

    b. Install a specific version using the version string from the second column,
       for example, `5:18.09.1~3-0~ubuntu-xenial`.

    ```bash
    $ sudo apt-get install docker-ce=<VERSION_STRING> docker-ce-cli=<VERSION_STRING> containerd.io
    ```

4.  Verify that Docker Engine - Community is installed correctly by running the `hello-world`
    image.

    ```bash
    $ sudo docker run hello-world
    ```

    This command downloads a test image and runs it in a container. When the
    container runs, it prints an informational message and exits.

Docker Engine - Community is installed and running. The `docker` group is created but no users
are added to it. You need to use `sudo` to run Docker commands.
Continue to [Linux postinstall](https://docs.docker.com/install/linux/linux-postinstall.md) to allow
non-privileged users to run Docker commands and for other optional configuration
steps.
