"""Random operations under random faults: the cluster still converges.

Uploads, edits, tag changes, deletes and downloads are issued at random nodes
while the network splits, cuts links one way, and crashes and restarts nodes.
Every response is checked as it happens, and the no-resurrection and GC safety
invariants after every step. Once healed, every node must hold the same
metadata, that metadata must be one the acknowledged operations allow, and
every live file must be readable from every node.
"""
from tests.chaos.scenario import ChaosRun, Profile


def test_random_faults_converge(chaos_cluster, seed, request):
    run = ChaosRun(chaos_cluster, seed, Profile())
    with run.reporting(request.node.nodeid):
        run.run_chaos()
        run.heal_and_converge()
