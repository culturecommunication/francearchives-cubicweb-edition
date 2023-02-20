/*
 * Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2019
 * Contact http://www.logilab.fr -- mailto:contact@logilab.fr
 *
 * This software is governed by the CeCILL-C license under French law and
 * abiding by the rules of distribution of free software. You can use,
 * modify and/ or redistribute the software under the terms of the CeCILL-C
 * license as circulated by CEA, CNRS and INRIA at the followingse URL
 * "http://www.cecill.info".
 *
 * As a counterpart to the access to the source code and rights to copy,
 * modify and redistribute granted by the license, users are provided only
 * with a limited warranty and the software's author, the holder of the
 * economic rights, and the successive licensors have only limited liability.
 *
 * In this respect, the user's attention is drawn to the risks associated
 * with loading, using, modifying and/or developing or reproducing the
 * software by the user in light of its specific status of free software,
 * that may mean that it is complicated to manipulate, and that also
 * therefore means that it is reserved for developers and experienced
 * professionals having in-depth computer knowledge. Users are therefore
 * encouraged to load and test the software's suitability as regards their
 * requirements in conditions enabling the security of their systemsand/or
 * data to be ensured and, more generally, to use and operate it in the
 * same conditions as regards security.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL-C license and that you accept its terms.
 */

import React, {useEffect} from 'react'

const {useForm} = require('react-hook-form')

const PropTypes = require('prop-types')

const {spinner: Spinner} = require('./fa')
const BootstrapTable = require('react-bootstrap-table-next').default

const {
    default: ToolkitProvider,
    Search,
    CSVExport,
} = require('react-bootstrap-table2-toolkit/dist/react-bootstrap-table2-toolkit.min')
const {
    default: paginationFactory,
    PaginationProvider,
    PaginationTotalStandalone,
} = require('react-bootstrap-table2-paginator')

const filterFactory = require('react-bootstrap-table2-filter').default

const Moment = require('moment')
Moment.locale('fr')

const {
    default: {jsonFetch, createEntity},
} = require('../api')

function BlacklistIndexForm(props) {
    const {
        register,
        handleSubmit,
        trigger,
        reset,
        getValues,
        watch,
        formState: {errors},
    } = useForm()

    // candidates: {
    //     eid: number,
    //     url: string
    //     label: string,
    //     type: string,
    //     count: string,
    // }[]
    const [candidates, setCandidates] = React.useState(null)

    const watchLabel = watch('label')

    React.useEffect(() => {
        setCandidates(null)
    }, [watchLabel])

    const onSubmit = (values) => {
        if (candidates === null) {
            // do not submit the form without checking for candidates
            return false
        }
        props.addLabel(values)
        resetForm()
    }

    function resetForm() {
        //clean the whole form
        setCandidates(null)
        reset()
    }

    function writeCandidates() {
        let options
        const title = (
            <label className="mt-0">Liste des autorités trouvées</label>
        )
        let help
        if (candidates !== null && candidates !== false) {
            if (candidates.length > 0) {
                options = candidates.map((obj) => (
                    <div className="form-check" key={obj['eid']}>
                        <input
                            type="checkbox"
                            {...register('autheids')}
                            className="form-check-input"
                            value={obj['eid']}
                        />
                        <label className="form-check-label" htmlFor="autheids">
                            <a
                                href={obj['url']}
                                target="_blank"
                                rel="noopener noreferrer"
                            >
                                {obj['label']} ({obj['eid']})
                            </a>{' '}
                            {obj['type']}, {obj['count']} documents(s)
                        </label>
                    </div>
                ))
                help = (
                    <div id="typeHelp" className="text-red mb-4">
                        Les autorités sélectionnées seront supprimées à l'ajout
                        du libellé banni.
                    </div>
                )
            } else {
                options = <div>Aucune autorité trouvée</div>
            }
            return (
                <div className="mb-4">
                    {title}
                    {help} {options}{' '}
                </div>
            )
        }
    }

    const getCandidatesAuthoritiesFromURLOrLabel = (values) => {
        jsonFetch(`/show-blacklisted-candidates`, {
            method: 'PUT',
            body: JSON.stringify(values),
        }).then((res) => {
            setCandidates(res)
        })
    }

    return (
        <div>
            <h2>Ajouter un libellé à bannir</h2>
            <form onSubmit={handleSubmit(onSubmit)} className="well">
                <div className="row">
                    <div className="mb-2">
                        <label className="form-label" htmlFor="label">
                            Libellé à bannir pour les autorités Sujets:
                            <span
                                className="required"
                                title="Ce champ est requis"
                            ></span>
                        </label>
                        <input
                            name="label"
                            className="form-control"
                            type="text"
                            autoComplete="off"
                            tabIndex="1"
                            {...register('label', {required: true})}
                        />

                        <div className="mt-2 mb-2 text-red">
                            {errors.label && 'Ce champ est obligatoire'}
                        </div>
                        <div id="typeHelp" className="form-text">
                            Saisir le libellé exact. Aucune normalisation sur le
                            libellé ne sera faite. Les autorités sujets
                            existantes avec exactement ce libellé seront rendues
                            orphelines et de nouvelles autorités ne seront plus
                            créées.
                        </div>
                    </div>
                </div>
                {writeCandidates()}
                <div className="btn-group">
                    <button
                        className="btn btn-default"
                        type="reset"
                        tabIndex="3"
                        onClick={() => resetForm()}
                    >
                        Annuler
                    </button>
                    <button
                        className="btn btn-default"
                        tabIndex="4"
                        type="button"
                        onClick={async () => {
                            if (await trigger()) {
                                getCandidatesAuthoritiesFromURLOrLabel(
                                    getValues(),
                                )
                            }
                            return false
                        }}
                    >
                        Rechercher les autorités concernées
                    </button>
                    {candidates !== null ? (
                        <button
                            className="btn btn-default"
                            type="submit"
                            tabIndex="5"
                        >
                            Ajouter à la liste des libellés bannis
                        </button>
                    ) : (
                        <span></span>
                    )}
                </div>
            </form>
        </div>
    )
}

BlacklistIndexForm.propTypes = {
    addLabel: PropTypes.func,
}

export function BlacklistedAuthorities() {
    // data = {label: string}[]
    const [data, setData] = React.useState([])
    const [message, setMessage] = React.useState(null)
    const [loading, setLoading] = React.useState(true)

    useEffect(() => {
        loadData()
    }, [])

    function loadData() {
        setLoading(true)
        jsonFetch('/get-blacklisted').then(
            (data) => setData(data),
            setLoading(false),
        )
    }

    function deleteLabel(row) {
        jsonFetch(`/remove-blacklisted`, {
            method: 'PUT',
            body: JSON.stringify(row),
        })
            .then(() => {
                setMessage(
                    `Libellé "${row['label']}" a été supprimé de la liste des index bannis.`,
                )
            })
            .then(() => {
                loadData()
            })
    }

    function addLabel(values) {
        // value : {label: "string", autheids: string|string[]}
        var authorities_to_delete = values['autheids']
        if (authorities_to_delete !== undefined) {
            if (!Array.isArray(authorities_to_delete)) {
                authorities_to_delete = [authorities_to_delete]
            }
            if (authorities_to_delete.length > 0) {
                const confirmation = confirm(
                    `Vous allez supprimer le(s) ${authorities_to_delete.length} autorité(s). Cette opération est irréversible. Voulez-vous continuer ?`,
                )
                if (!confirmation) {
                    return
                }
                // launch delete authorities task
                let tasks_infos = []
                authorities_to_delete.map((autheid) =>
                    createEntity(
                        'RqTask',
                        {
                            name: `remove_authorities`,
                            title: `remove authority #${autheid} "${values['label']}"`,
                            authority_eid: autheid,
                        },
                        'remove_authorities',
                    ).then((doc) => {
                        if (doc.errors && doc.errors.length) {
                            tasks_infos.push(doc.errors)
                        } else {
                            tasks_infos.push(
                                `Une tâche de suppression de l'autorité #${autheid} a été créée`,
                            )
                        }
                    }),
                )
            }
        }
        jsonFetch(`/add-blacklisted`, {
            method: 'PUT',
            body: JSON.stringify(values),
        })
            .then(() => {
                loadData()
            })
            .then(() => {
                setMessage(
                    `Libellé "${values['label']}" a été ajouté à la liste des index bannis. Suivre les tâches de désindexation des autorités dans le tableau des tâches.`,
                )
            })
    }

    function displayBoostrapTable() {
        // pagination

        const paginationOptions = {
            paginationSize: 10,
            showTotal: false,
            hidePageListOnlyOnePage: true,
            sizePerPageList: [
                {
                    text: '20',
                    value: 20,
                },
                {
                    text: '30',
                    value: 30,
                },
                {
                    text: '40',
                    value: 40,
                },
                {
                    text: 'Tout',
                    value: data.length,
                },
            ], // A numeric array is also available. the purpose of above example is custom the text
        }

        const {SearchBar} = Search

        const standaloneTotal = (from, to, size) => (
            <h2>
                {size < 2
                    ? `Liste des libellés bannis (${size} libellé)`
                    : `Liste des libellés bannis (${size} libellés)`}
            </h2>
        )

        const deleteButton = (row) => (
            <button className="btn btn-xs" onClick={() => deleteLabel(row)}>
                <i className="fa fa-trash"></i>
            </button>
        )

        const columns = [
            {
                dataField: 'label',
                text: 'Libellé',
                searchable: true,
                csvExport: true,
                sort: true,
            },
            {
                dataField: 'delete',
                text: 'Action',
                formatter: (_cellContent, row) => deleteButton(row),
                csvExport: false,
                headerStyle: () => {
                    return {width: '10%'}
                },
            },
        ]
        const {ExportCSVButton} = CSVExport

        const csvFilename =
            'blacklisted_indexes' + Moment().format('YYYYMMDD') + '.csv'

        return (
            <PaginationProvider
                pagination={paginationFactory(paginationOptions)}
            >
                {(pageprops) => (
                    <div>
                        <PaginationTotalStandalone
                            {...pageprops.paginationProps}
                            paginationTotalRenderer={standaloneTotal}
                        />
                        <ToolkitProvider
                            keyField="label"
                            data={data}
                            columns={columns}
                            search
                            bootstrap4
                            exportCSV={{
                                onlyExportFiltered: true,
                                fileName: csvFilename,
                                exportAll: false,
                            }}
                        >
                            {(tkprops) => (
                                <div>
                                    <SearchBar
                                        {...tkprops.searchProps}
                                        placeholder="rechercher dans les libellés bannis"
                                    />
                                    <BootstrapTable
                                        {...tkprops.baseProps}
                                        {...pageprops.paginationTableProps}
                                        filter={filterFactory()}
                                        filterPosition="top"
                                        striped
                                        noDataIndication={'aucun résultat'}
                                    />
                                    <ExportCSVButton {...tkprops.csvProps}>
                                        Export CSV
                                    </ExportCSVButton>
                                </div>
                            )}
                        </ToolkitProvider>
                    </div>
                )}
            </PaginationProvider>
        )
    }

    return (
        <div>
            <h1>Gestion des libellés bannis pour les autorités Sujects</h1>
            <BlacklistIndexForm addLabel={addLabel} />
            <hr />
            {message ? <div className="alert alert-info"> {message} </div> : ``}
            {loading ? <Spinner /> : displayBoostrapTable()}
        </div>
    )
}
