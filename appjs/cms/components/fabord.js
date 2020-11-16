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

const React = require('react')
const {Component} = require('react')
const PropTypes = require('prop-types')

const {spinner: Spinner} = require('./fa'),
    BootstrapTable = require('react-bootstrap-table-next').default

const {
    default: ToolkitProvider,
    Search,
    CSVExport,
} = require('react-bootstrap-table2-toolkit')
const {
    default: paginationFactory,
    PaginationProvider,
    PaginationTotalStandalone,
} = require('react-bootstrap-table2-paginator')

const filterFactory = require('react-bootstrap-table2-filter').default
const {selectFilter} = require('react-bootstrap-table2-filter')

const Moment = require('moment')
Moment.locale('fr')

const {
    default: {jsonFetch},
} = require('../api')

const {default: Select} = require('react-select')

function linkFormatter(cell) {
    return (
        <a href={cell[1]} _target="blank">
            {cell[0]}
        </a>
    )
}

function dateTimeFormatter(date) {
    const mdate = Moment(date)
    if (mdate && mdate._isValid) {
        return Moment(mdate).format('DD/MM/YYYY à hh:mm') // '14/10/2019 à 07:01'
    }
    return date
}

function csvDateTimeFormatter(cell) {
    return dateTimeFormatter(cell)
}

function SelectService({onChange, options}) {
    return (
        <div>
            <div className="field-title">Sélectionnez un service :</div>
            <Select
                options={options}
                isMulti={false}
                isClearable={true}
                isSearchable={true}
                placeholder="Sélectionnez un service"
                noOptionsMessage={() => 'aucun serivce trouvé'}
                onChange={ev => onChange(ev)}
            />
        </div>
    )
}
SelectService.propTypes = {
    onChange: PropTypes.func.isRequired,
    options: PropTypes.arrayOf(
        PropTypes.shape({
            value: PropTypes.string.isRequired,
            label: PropTypes.string.isRequired,
        }),
    ).isRequired,
}

class FAMonitoringBord extends Component {
    constructor(props, ctx) {
        super(props, ctx)
        this.state = {data: null, selectedService: null, options: null}
        this.updateSelectedService = this.updateSelectedService.bind(this)
        this.displayBoostrapTable = this.displayBoostrapTable.bind(this)
    }

    componentDidMount() {
        jsonFetch('/faservices').then(services => {
            const options = services.map(s => ({value: s.code, label: s.name}))
            this.setState({options})
        })
    }

    updateSelectedService(option) {
        this.setState({data: null, selectedService: option})
        if (option !== null) {
            jsonFetch('/faforservice?service=' + option.value).then(data =>
                this.setState({data}),
            )
        }
    }

    displayBoostrapTable() {
        const {selectedService, data} = this.state
        // pagination
        const customTotal = (from, to, size) => (
                <span className="react-bootstrap-table-pagination-total">
                    Résultats {from} à {to} sur {size}
                </span>
            ),
            paginationOptions = {
                paginationSize: 10,
                pageStartIndex: 1,
                // alwaysShowAllBtns: true, // Always show next and previous button
                // withFirstAndLast: false, // Hide the going to First and Last page button
                // hideSizePerPage: true, // Hide the sizePerPage dropdown always
                // hidePageListOnlyOnePage: true, // Hide the pagination list when only one page
                paginationTotalRenderer: customTotal,
                showTotal: true,
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
                        text: '40',
                        value: 40,
                    },
                    {
                        text: 'Tout',
                        value: data.length,
                    },
                ], // A numeric array is also available. the purpose of above example is custom the text
            }
        const standaloneTotal = (from, to, size) => (
            <div className="field-title">
                {size < 2
                    ? `${size} instrument de recherche trouvé`
                    : `${size} instruments de recherche trouvés`}
            </div>
        )
        // search
        const {SearchBar} = Search
        const statusSelectOptions = {
            brouillon: 'brouillon',
            publié: 'publié',
        }
        const importSelectOptions = {
            ZIP: 'ZIP',
            OAI: 'OAI',
        }
        // sort
        const defaultSorted = [
            {
                dataField: 'creation_date',
                order: 'desc',
            },
        ]
        const {ExportCSVButton} = CSVExport
        const columns = [
            {
                dataField: 'eid',
                text: '',
                hidden: true,
                searchable: false,
                csvExport: false,
            },
            {
                dataField: 'url',
                text: 'EADID',
                formatter: linkFormatter,
                csvFormatter: cell => `${cell[0]}`,
                sort: true,
            },
            {
                dataField: 'stable_id',
                text: 'StableId',
                sort: true,
            },
            {
                dataField: 'name',
                text: 'Titre',
                sort: true,
            },
            {
                dataField: 'filename',
                text: 'Nom du fichier',
                sort: true,
            },
            {
                dataField: 'import',
                text: "Type d'import",
                sort: true,
                formatter: cell => importSelectOptions[cell],
                filter: selectFilter({
                    options: importSelectOptions,
                    defaultValue: 0,
                }),
            },
            {
                dataField: 'status',
                text: 'Statut',
                sort: true,
                formatter: cell => statusSelectOptions[cell],
                filter: selectFilter({
                    options: statusSelectOptions,
                    defaultValue: 0,
                }),
            },
            {
                dataField: 'creation_date',
                text: 'Date de création',
                formatter: dateTimeFormatter,
                csvFormatter: csvDateTimeFormatter,
                sort: true,
            },
            {
                dataField: 'url',
                csvText: 'URL',
                hidden: true,
                csvFormatter: cell => `${cell[1]}`,
            },
        ]
        const csvFilename =
            selectedService.value + '_' + Moment().format('YYYYMMDD') + '.csv'

        return (
            <PaginationProvider
                pagination={paginationFactory(paginationOptions)}
            >
                {pageprops => (
                    <div>
                        <PaginationTotalStandalone
                            {...pageprops.paginationProps}
                            paginationTotalRenderer={standaloneTotal}
                        />
                        <ToolkitProvider
                            keyField="tabord"
                            data={data}
                            columns={columns}
                            defaultSorted={defaultSorted}
                            search
                            exportCSV={{
                                onlyExportFiltered: true,
                                fileName: csvFilename,
                                exportAll: false,
                            }}
                        >
                            {tkprops => (
                                <div>
                                    <SearchBar
                                        {...tkprops.searchProps}
                                        placeholder="rechercher dans les IRs"
                                    />
                                    <hr />
                                    <ExportCSVButton {...tkprops.csvProps}>
                                        Export CSV
                                    </ExportCSVButton>
                                    <hr />
                                    <BootstrapTable
                                        {...tkprops.baseProps}
                                        defaultSorted={defaultSorted}
                                        {...pageprops.paginationTableProps}
                                        filter={filterFactory()}
                                    />
                                </div>
                            )}
                        </ToolkitProvider>
                    </div>
                )}
            </PaginationProvider>
        )
    }

    render() {
        const {selectedService, options, data} = this.state
        let services
        if (options !== null) {
            services = (
                <SelectService
                    options={options}
                    onChange={this.updateSelectedService}
                />
            )
        } else {
            services = <Spinner />
        }
        let table = null
        if (selectedService !== null) {
            if (data === null) {
                table = <Spinner />
            } else {
                table = <div>{this.displayBoostrapTable()}</div>
            }
        }
        return (
            <div>
                <h1>Tableau de suivi des Instruments de recherche</h1>
                <div>{services}</div>
                {table}
            </div>
        )
    }
}

module.exports = FAMonitoringBord
