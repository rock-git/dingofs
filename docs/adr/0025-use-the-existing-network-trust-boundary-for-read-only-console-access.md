# Use the existing network trust boundary for read-only console access

The initial read-only Management API and React console will not add application-layer authentication; like the existing bRPC diagnostics, they rely on the MDS HTTP port being reachable only from a trusted operations network. Responses must exclude credentials and deployment documentation must make this exposure explicit, and this decision must be revisited before any mutating management action is introduced.
