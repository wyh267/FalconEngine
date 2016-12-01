#!/bin/bash

go install FalconSearch

cp bin/FalconSearch deploy/node1/
cp bin/FalconSearch deploy/node2/
cp bin/FalconSearch deploy/node3/
cp bin/FalconSearch deploy/nodeM/

