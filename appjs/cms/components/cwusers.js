/*
 * Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2019
 * Contact http://www.logilab.fr -- mailto:contact@logilab.fr
 *
 * This software is governed by the CeCILL-C license under French law and
 * abiding by the rules of distribution of free software. You can use,
 * modify and/ or redistribute the software under the terms of the CeCILL-C
 * license as circulated by CEA, CNRS and INRIA at the following URL
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

const {spinner: Spinner} = require('./fa')

const BootstrapTable = require('react-bootstrap-table-next').default

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

const {
    default: {jsonFetch},
} = require('../api')

function linkFormatter(cell, cwuser) {
    return (
        <a href={cwuser.absoluteUrl} _target="_blank" rel="noopener noreferrer">
            {cell}
        </a>
    )
}

export function SearchCWUsers() {
    const [data, setData] = React.useState([])
    const [loading, setLoading] = React.useState(false)

    useEffect(() => {
        loadData()
    }, [])

    function loadData() {
        setLoading(true)
        jsonFetch('/cwusers').then((data) => setData(data), setLoading(false))
    }

    function displayBoostrapTable() {
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
                    value: 30,
                },
                {
                    text: 'Tout',
                    value: data.length,
                },
            ], // A numeric array is also available. the purpose of above example is custom the text
        }

        const {SearchBar} = Search

        const standaloneTotal = (from, to, size) => (
            <h2>{size < 2 ? `${size} utilisateur` : `${size} utilisateurs`}</h2>
        )

        const columns = [
            {
                dataField: 'eid',
                text: 'eid',
                hidden: true,
            },
            {
                dataField: 'login',
                text: 'Identifiant',
                searchable: true,
                sort: true,
                formatter: linkFormatter,
            },
            {
                dataField: 'firstname',
                text: 'Nom',
                searchable: true,
                sort: true,
            },
            {
                dataField: 'surname',
                text: 'Prénom',
                searchable: true,
                sort: true,
            },
            {
                dataField: 'in_group_name',
                text: 'Groupe',
                searchable: true,
                sort: true,
            },
            {
                dataField: 'use_email',
                text: 'Courriel',
                searchable: true,
                sort: true,
                formatter: (cell, cwuser) => {
                    return cwuser.use_email !== undefined
                        ? cwuser.use_email.map((e) => e.address).join(', ')
                        : null
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
                            keyField="label"
                            data={data}
                            columns={columns}
                            search
                            bootstrap4
                        >
                            {(tkprops) => (
                                <div>
                                    <SearchBar
                                        {...tkprops.searchProps}
                                        placeholder="rechercher des utilsateurs par nom, prénom, groupe, courriel"
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

    return (
        <div>
            <h1>Recherche des utilisateurs</h1>
            <hr />
            {loading ? <Spinner /> : displayBoostrapTable()}
        </div>
    )
}
