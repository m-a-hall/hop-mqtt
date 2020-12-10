Hop MQTT IoT Plugin
=======================

The Hop MQTT Project is a plugin for the Hop platform which provides the ability to subscribe to, and publish to, MQTT topics on a MQTT broker. Adapted from
the original Pentaho data integration version. Includes enhancements courtesy of Jean-Francois Monteil (see his blog at: https://www.linkedin.com/pulse/exploring-mqtt-cool-features-pentaho-demo-jean-francois-monteil/).

Building
--------
The Hop MQTT plugin is built using Maven.

    $ git clone git://github.com/m-a-hall/hop-mqtt.git
    $ cd hop-mqtt
    $ mvn install

This will produce a plugin archive in the target directory. This archive can then be extracted into your Hop plugins/transforms directory.

License
-------
Licensed under the Apache License, Version 2.0. See LICENSE.txt for more information.
