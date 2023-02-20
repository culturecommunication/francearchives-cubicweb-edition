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

const PropTypes = require('prop-types')

const {spinner: Spinner} = require('./fa')
const BootstrapTable = require('react-bootstrap-table-next').default
import cellEditFactory, {Type} from 'react-bootstrap-table2-editor'

const {
    default: ToolkitProvider,
    Search,
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
    default: {jsonFetch},
} = require('../api')

const {Alert} = require('../components/error')

SectionThemes.propTypes = {
    entity: PropTypes.object,
}

export function SectionThemes(props) {
    const entity = props.entity.toJS()
    const [availableThemes, setAvailableThemes] = React.useState([])
    const [selectedThemes, setSelectedThemes] = React.useState([])
    const [messageAdd, setMessageAdd] = React.useState(null)
    const [messageDelete, setMessageDelete] = React.useState(null)
    const [loading, setLoading] = React.useState(true)

    useEffect(() => {
        loadData()
    }, [])

    function loadData() {
        setLoading(true)
        jsonFetch(`/sectionthemes/${entity.eid}`).then((data) => {
            setAvailableThemes(data['available'])
            setSelectedThemes(data['selected'])
        }, setLoading(false))
    }

    function addTheme(row) {
        jsonFetch(`/add-sectiontheme/${entity.eid}`, {
            method: 'PUT',
            body: JSON.stringify(row),
        })
            .then(() => {
                setMessageDelete({})
                setMessageAdd({
                    type: 'success',
                    text: `Le thème "${row['label'][0]}" (${row['label'][1]}) a été ajouté sur la liste des thèmes de la rubrique.`,
                })
            })
            .then(() => {
                loadData()
            })
            .catch((e) => {
                setMessageDelete({})
                setMessageAdd({
                    type: 'danger',
                    text: `Le theme "${row['label'][0]}" n'a pas pu être ajouté : ${e}.`,
                })
                console.error(e)
            })
    }

    function deleteTheme(row) {
        jsonFetch(`/delete-sectiontheme/${entity.eid}`, {
            method: 'PUT',
            body: JSON.stringify(row),
        })
            .then(() => {
                setMessageAdd({})
                setMessageDelete({
                    type: 'success',
                    text: `Le thème "${row['label'][0]}" (${row['label'][1]}) a été supprimé de la liste des thèmes de la rubrique.`,
                })
            })
            .then(() => {
                loadData()
            })
            .catch((e) => {
                setMessageAdd({})
                setMessageDelete({
                    type: 'danger',
                    text: `Le theme "${row['label'][0]}" n'a pas pu être supprimé : ${e}`,
                })
                console.error(e)
            })
    }

    function modifiyOrderTheme(row) {
        jsonFetch(`/modify-subjecttheme/${row.eid}`, {
            method: 'PUT',
            body: JSON.stringify(row),
        })
            .then(() => {
                setMessageAdd({})
                setMessageDelete({
                    type: 'success',
                    text: `L'ordre du thème "${row['label'][0]}" (${row['label'][1]}) a été modifié.`,
                })
            })
            .then(() => {
                loadData()
            })
            .catch((e) => {
                setMessageAdd({})
                setMessageDelete({
                    type: 'danger',
                    text: `L'ordre du thème "${row['label'][0]}" n'a pas être modifié : ${e}`,
                })
                console.error(e)
            })
    }

    const paginationOptions = {
        paginationSize: 10,
        showTotal: false,
        hidePageListOnlyOnePage: true,
        sizePerPageList: [
            {
                text: '10',
                value: 10,
            },
            {
                text: '20',
                value: 20,
            },
            {
                text: '30',
                value: 40,
            },
            {
                text: 'Tout',
                value: availableThemes.length,
            },
        ], // A numeric array is also available. the purpose of above example is custom the text
    }

    function displayAvailableThemes() {
        // pagination

        const {SearchBar} = Search

        const standaloneTotal = (from, to, size) => (
            <div>
                <h2>Liste des thèmes disponibles ({size})</h2>
                <div id="typeHelp" className="form-text mt-2 mb-4">
                    Cette liste présente les thèmes liés aux contenus éditoriaux
                    présents dans la rubrique. Cliquez sur le "+" dans la
                    colonne "Ajouter" pour sélectionner un thème et le faire
                    afficher sur la page de la rubrique.
                </div>
            </div>
        )

        const columns = [
            {
                dataField: 'label',
                text: 'Thème',
                searchable: true,
                formatter: function formatLink(cell) {
                    return (
                        <a
                            href={cell[1]}
                            target="_blank"
                            rel="noopener noreferrer"
                        >
                            {cell[0]}
                        </a>
                    )
                },
                sort: true,
            },
            {
                dataField: 'count',
                text: 'Documents liés',
                sort: true,
                headerStyle: () => {
                    return {width: '10%'}
                },
            },
            {
                dataField: 'eid',
                text: 'Ajouter',
                formatter: function formatAddThemeBtn(_cellContent, row) {
                    return (
                        <button
                            className="btn btn-xs btn-default"
                            onClick={() => addTheme(row)}
                        >
                            <i className="fa fa-plus"></i>
                        </button>
                    )
                },
                headerStyle: () => {
                    return {width: '10%'}
                },
            },
        ]

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
                            keyField="eid"
                            data={availableThemes}
                            columns={columns}
                            search
                            bootstrap4
                        >
                            {(tkprops) => (
                                <div>
                                    <SearchBar
                                        {...tkprops.searchProps}
                                        placeholder="rechercher dans les thèmes"
                                    />
                                    <BootstrapTable
                                        {...tkprops.baseProps}
                                        {...pageprops.paginationTableProps}
                                        filter={filterFactory()}
                                        filterPosition="top"
                                        striped
                                        noDataIndication={'aucun résultat'}
                                    />
                                </div>
                            )}
                        </ToolkitProvider>
                    </div>
                )}
            </PaginationProvider>
        )
    }

    function displaySelectedThemes() {
        // pagination

        const {SearchBar} = Search

        const standaloneTotal = (from, to, size) => (
            <div>
                <h2>Liste des thèmes selectionnés ({size})</h2>
                <div id="typeHelp" className="form-text mt-2 mb-4">
                    Cette liste présente les thèmes sélectionnés. Cliquez sur
                    l'icône dans la colonne "Supprimer" pour retirer un thème de
                    la page de la rubrique. Cliquez sur la valeur de la colonne
                    "Ordre d'affichage" pour modifier l'ordre.
                </div>
            </div>
        )

        const cellEdit = cellEditFactory({
            mode: 'click',
            blurToSave: true,

            afterSaveCell: (oldValue, newValue, row) => {
                modifiyOrderTheme(row)
            },
        })

        const columns = [
            {
                dataField: 'label',
                text: 'Thème',
                editable: false,
                searchable: true,
                formatter: function formatLink(cell) {
                    return (
                        <a
                            href={cell[1]}
                            target="_blank"
                            rel="noopener noreferrer"
                        >
                            {cell[0]}
                        </a>
                    )
                },
                sort: true,
            },
            {
                dataField: 'count',
                text: 'Documents liés',
                editable: false,
                sort: true,
                headerStyle: () => {
                    return {width: '20%'}
                },
            },
            {
                dataField: 'order',
                text: "Ordre d'affichage",
                type: 'number',
                editor: {type: Type.TEXTAREA},
                validator: (newValue) => {
                    if (isNaN(newValue)) {
                        return {
                            valid: false,
                            message: 'La valeur doit être un entier',
                        }
                    }
                    return true
                },
                sort: true,
                headerStyle: () => {
                    return {width: '10%'}
                },
            },
            {
                dataField: 'eid',
                text: 'Supprimer',
                editable: false,
                formatter: function formatDeleteThemeBtn(_cellContent, row) {
                    return (
                        <button
                            className="btn btn-xs btn-default"
                            onClick={() => deleteTheme(row)}
                        >
                            <i className="fa fa-trash"></i>
                        </button>
                    )
                },
                headerStyle: () => {
                    return {width: '10%'}
                },
            },
        ]

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
                            keyField="eid"
                            data={selectedThemes}
                            columns={columns}
                            search
                            bootstrap4
                        >
                            {(tkprops) => (
                                <div>
                                    <SearchBar
                                        {...tkprops.searchProps}
                                        placeholder="rechercher dans les thèmes"
                                    />
                                    <BootstrapTable
                                        {...tkprops.baseProps}
                                        {...pageprops.paginationTableProps}
                                        cellEdit={cellEdit}
                                        filter={filterFactory()}
                                        filterPosition="top"
                                        striped
                                        noDataIndication={'aucun résultat'}
                                    />
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
            <h1>Gestion des thèmes de la rubrique "{entity.dc_title}"</h1>
            <hr />
            <Alert message={messageAdd} />
            {loading ? <Spinner /> : displayAvailableThemes()}
            <Alert message={messageDelete} />
            {loading ? <Spinner /> : displaySelectedThemes()}
        </div>
    )
}
