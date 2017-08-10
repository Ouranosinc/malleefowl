.. _configuration:

Configuration
*************
If you want to run on a different hostname or port then change the default values in ``custom.cfg``:

.. code-block:: ini

   $ cd malleefowl
   $ vim custom.cfg
   $ cat custom.cfg
   [settings]
   hostname = localhost
   http-port = 8091

After any change to your ``custom.cfg`` you **need** to run ``make update`` again and restart the ``supervisor`` service:

.. code-block:: sh

   $ make update    # or install
   $ make restart
   $ make status
