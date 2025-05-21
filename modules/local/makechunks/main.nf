process MAKECHUNKS {
    tag "$meta.id"
    label 'process_single'

    conda "${moduleDir}/environment.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://community-cr-prod.seqera.io/docker/registry/v2/blobs/sha256/cc/cc8f3f46536ba563bdd43fe73dda1dd6699a22eaf1faef037b6f23526a6e09ea/data':
        'community.wave.seqera.io/library/polars:1.24.0--2d2d323e8514e707' }"

    input:
    tuple val(meta), path(parquet)

    output:
    tuple val(meta), path("*_chunks_*.pq"), emit: parquet
    path "versions.yml"                   , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    """
    make_chunks.py \\
        --parquet ${parquet} \\
        --prefix ${prefix} \\
        ${args}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        Python: \$(python --version |& sed '1!d ; s/Python //')
    END_VERSIONS
    """

    stub:
    def prefix = task.ext.prefix ?: "${meta.id}"
    """
    touch ${prefix}_chunks_1000.pq

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        Python: \$(python --version |& sed '1!d ; s/Python //')
    END_VERSIONS
    """
}
