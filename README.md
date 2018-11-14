# dispytcher: Job dispatcher in Python

I got sick and tired of sshing into different servers and run simulations, and I
don't want to go through all the trouble installing HPC softwares such as slurm.
So this is my solution: sending configurations in JSON format over sockets and
the receiver start the simulation process.

There is no fancy scheduling here, everything is static.

This is a very specific use case and I didn't expect it to expand beyond this
designed capability.

## Prerequisites

- Python3 on all worker and dispatcher systems.
- A shared file system across workers (everyone sees the same path).
- Preferrably the dispatcher and workers are in the same network, for security reasons.
